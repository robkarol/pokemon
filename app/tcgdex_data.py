"""
Downloads card metadata and art from TCGdex (tcgdex.dev), a community-run
multi-language Pokemon TCG database. pokemontcg.io (see pokemon_data.py)
only covers English cards, so this is the source for everything else —
currently Japanese and Thai.

TCGdex has no bulk "all cards in a set, fully detailed" endpoint: a set
lookup only returns brief {id, localId, name} entries, so every card needs
its own detail request to get rarity/types/hp/image/etc. That's a lot more
individual requests than the pokemontcg.io sync, but TCGdex has held up
fine under moderate concurrency in testing (unlike pokemontcg.io's
frequent 5xx), so this fetches card details and images concurrently per
set rather than pacing every request like the English sync does.

Card art coverage is partial and varies a lot by set/language (Japanese
in particular — many older/obscure sets have no scans at all on TCGdex);
cards without an "image" field are still stored with their metadata, just
without art. IDs are prefixed with `tcgdex-<lang>-` to keep them from ever
colliding with pokemontcg.io's English card IDs.
"""
import asyncio
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from app.database import existing_set_keys, get_connection, log_sync, upsert_cards

logger = logging.getLogger(__name__)

API_BASE = "https://api.tcgdex.net/v2"
IMAGES_DIR = Path("/app/images")
LANGUAGES = ["ja", "th"]
WORKERS = 8
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
IMAGE_QUALITY = "high"
IMAGE_EXT = "png"

CATEGORY_TO_SUPERTYPE = {
    "Pokemon": "Pokémon",
    "Trainer": "Trainer",
    "Energy": "Energy",
}

_sync_status: dict = {
    "running": False,
    "new_only": False,
    "phase": None,
    "language": None,
    "current_set": None,
    "sets_done": 0,
    "total_sets": None,
    "sets_failed": 0,
    "cards_synced": 0,
    "images_downloaded": 0,
    "last_synced": None,
    "last_error": None,
}


def get_sync_status() -> dict:
    return dict(_sync_status)


def _request_json(url: str, max_retries: int = 6) -> Optional[dict]:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=20)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            time.sleep(min(0.5 * (attempt + 1), 10))
            continue

        if resp.status_code == 404:
            return None
        if resp.status_code in RETRYABLE_STATUS:
            wait = float(resp.headers.get("Retry-After", min(0.5 * (2 ** attempt), 10)))
            logger.warning(f"{resp.status_code} from {url}, retrying in {wait:.1f}s")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} retries") from last_exc


def _extract_int(value) -> Optional[int]:
    if value is None:
        return None
    match = re.search(r"\d+", str(value))
    return int(match.group()) if match else None


def _download_image(dest_id: str, image_base_url: str) -> str:
    dest = IMAGES_DIR / f"{dest_id}.{IMAGE_EXT}"
    if dest.exists() and dest.stat().st_size > 0:
        return dest.name
    resp = requests.get(f"{image_base_url}/{IMAGE_QUALITY}.{IMAGE_EXT}", timeout=30)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return dest.name


def _card_subtypes(card: dict) -> list[str]:
    subtypes = []
    for key in ("stage", "trainerType", "energyType"):
        value = card.get(key)
        if value:
            subtypes.append(value)
    return subtypes


def _card_variants(card: dict) -> list[str]:
    flags = card.get("variants") or {}
    is_first_ed = bool(flags.get("firstEdition"))
    variants: set[str] = set()
    # first-edition normal and first-edition holo are distinct, separately
    # collectible prints from their non-first-edition counterparts, so fold
    # it into the variant name itself rather than as an independent flag.
    if flags.get("normal"):
        variants.add("first_edition_normal" if is_first_ed else "normal")
    if flags.get("holo"):
        variants.add("first_edition_holo" if is_first_ed else "holo")
    if flags.get("reverse"):
        variants.add("reverse_holo")
    if flags.get("wPromo"):
        variants.add("promo")
    if is_first_ed and not flags.get("normal") and not flags.get("holo"):
        variants.add("first_edition")
    return sorted(variants)


def _process_card(lang: str, card_id: str, set_meta: dict) -> Optional[tuple]:
    card = _request_json(f"{API_BASE}/{lang}/cards/{card_id}")
    if not card:
        return None

    dest_id = f"tcgdex-{lang}-{card_id}"
    image_base = card.get("image")
    image_filename = None
    if image_base:
        try:
            image_filename = _download_image(dest_id, image_base)
            _sync_status["images_downloaded"] += 1
        except Exception as exc:
            logger.warning(f"Image download failed for {dest_id}: {exc}")

    hp = card.get("hp")
    number = card.get("localId") or ""
    dex_numbers = card.get("dexId") or []
    supertype = CATEGORY_TO_SUPERTYPE.get(card.get("category"), card.get("category"))
    image_url = f"{image_base}/{IMAGE_QUALITY}.{IMAGE_EXT}" if image_base else None

    return (
        dest_id,
        card.get("name"),
        supertype,
        json.dumps(_card_subtypes(card)),
        json.dumps(card.get("types") or []),
        str(hp) if hp is not None else None,
        _extract_int(hp),
        card.get("rarity"),
        f"tcgdex-{lang}-{set_meta['id']}",
        set_meta["name"],
        set_meta["series"],
        set_meta["release_date"],
        number,
        _extract_int(number),
        card.get("illustrator"),
        dex_numbers[0] if dex_numbers else None,
        lang,
        json.dumps(_card_variants(card)),
        image_url,
        image_filename,
    )


def _sync_one_set(conn, pool: ThreadPoolExecutor, lang: str, set_brief: dict) -> int:
    detail = _request_json(f"{API_BASE}/{lang}/sets/{set_brief['id']}")
    if not detail:
        return 0
    cards = detail.get("cards") or []
    if not cards:
        return 0

    serie = detail.get("serie") or {}
    set_meta = {
        "id": detail["id"],
        "name": detail.get("name"),
        "series": serie.get("name"),
        "release_date": detail.get("releaseDate"),
    }

    futures = [pool.submit(_process_card, lang, c["id"], set_meta) for c in cards]
    rows = []
    for future in as_completed(futures):
        try:
            row = future.result()
            if row:
                rows.append(row)
        except Exception as exc:
            logger.warning(f"Card fetch failed in set {set_brief['id']} ({lang}): {exc}")

    if rows:
        upsert_cards(conn, rows)
    return len(rows)


def _sync_one_language(conn, pool: ThreadPoolExecutor, lang: str, new_only: bool = False) -> None:
    sets = _request_json(f"{API_BASE}/{lang}/sets") or []
    if new_only:
        known = existing_set_keys()
        sets = [s for s in sets if (f"tcgdex-{lang}-{s['id']}", lang) not in known]
    _sync_status["total_sets"] = len(sets)
    _sync_status["sets_done"] = 0
    _sync_status["sets_failed"] = 0

    for set_brief in sets:
        _sync_status["current_set"] = set_brief.get("name")
        try:
            synced = _sync_one_set(conn, pool, lang, set_brief)
            _sync_status["cards_synced"] += synced
        except Exception as exc:
            logger.warning(f"Skipping set {set_brief.get('id')} ({lang}): {exc}")
            _sync_status["sets_failed"] += 1
        finally:
            _sync_status["sets_done"] += 1


def sync_tcgdex_data(languages: Optional[list[str]] = None, new_only: bool = False) -> dict:
    """Walk every set for each language, upserting card metadata and
    downloading any card art not already cached on disk. With new_only,
    sets already in the database are skipped."""
    if _sync_status["running"]:
        return {"started": False, "message": "sync already running"}

    languages = languages or LANGUAGES
    _sync_status.update(
        running=True,
        new_only=new_only,
        phase="starting",
        language=None,
        current_set=None,
        sets_done=0,
        total_sets=None,
        sets_failed=0,
        cards_synced=0,
        images_downloaded=0,
        last_error=None,
    )
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for lang in languages:
                _sync_status["language"] = lang
                _sync_status["phase"] = f"syncing {lang}"
                _sync_one_language(conn, pool, lang, new_only)

        _sync_status["last_synced"] = datetime.now(timezone.utc).isoformat()
        log_sync("success", _sync_status["cards_synced"], _sync_status["images_downloaded"])
        return {
            "started": True,
            "cards_synced": _sync_status["cards_synced"],
            "images_downloaded": _sync_status["images_downloaded"],
        }

    except Exception as exc:
        logger.exception("TCGdex sync failed")
        _sync_status["last_error"] = str(exc)
        log_sync("error", _sync_status["cards_synced"], _sync_status["images_downloaded"], error=str(exc))
        raise

    finally:
        _sync_status["running"] = False
        _sync_status["phase"] = None
        _sync_status["current_set"] = None
        conn.close()


async def sync_tcgdex_data_async(languages: Optional[list[str]] = None, new_only: bool = False) -> dict:
    return await asyncio.to_thread(sync_tcgdex_data, languages, new_only)
