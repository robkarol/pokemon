"""
Downloads card metadata and art from the Pokemon TCG API (pokemontcg.io)
and caches both in SQLite + a flat images/ folder.

The API's unfiltered /v2/cards listing (no `q` filter) is unreliable at the
full collection's size — it intermittently 500s regardless of page size.
Querying scoped to one set at a time (`q=set.id:<id>`) is far more reliable,
so sync walks /v2/sets first and then paginates each set's cards.

Metadata is cheap to refetch, so a resync always walks every set and
upserts. Images are the expensive part, so a card's art is only ever
downloaded once: if its file already exists on disk, sync skips it. This
makes resyncing after new sets are released fast and cheap. The API itself
is also flaky in general (transient 429/500/502/503), so every request is
retried with backoff, and a set that still fails after retries is skipped
rather than aborting the whole sync.
"""
import asyncio
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

from app.database import existing_set_keys, get_connection, log_sync, upsert_cards

logger = logging.getLogger(__name__)

SETS_URL = "https://api.pokemontcg.io/v2/sets"
CARDS_URL = "https://api.pokemontcg.io/v2/cards"
API_KEY = os.environ.get("POKEMONTCG_API_KEY", "").strip()
IMAGES_DIR = Path("/app/images")
SETS_PAGE_SIZE = 100
CARDS_PAGE_SIZE = 250
IMAGE_DOWNLOAD_WORKERS = 5
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Without an API key the shared rate limit is much stricter, so pace requests
# far more conservatively. Override with POKEMONTCG_REQUEST_DELAY if needed.
_DEFAULT_DELAY = "0.6" if API_KEY else "6"
REQUEST_DELAY = float(os.environ.get("POKEMONTCG_REQUEST_DELAY", _DEFAULT_DELAY))

_sync_status: dict = {
    "running": False,
    "new_only": False,
    "phase": None,
    "current_set": None,
    "sets_done": 0,
    "total_sets": None,
    "sets_failed": 0,
    "cards_synced": 0,
    "total_cards": None,
    "images_downloaded": 0,
    "last_synced": None,
    "last_error": None,
}


def get_sync_status() -> dict:
    return dict(_sync_status)


def _headers() -> dict:
    return {"X-Api-Key": API_KEY} if API_KEY else {}


def _request_json(url: str, params: dict, max_retries: int = 16) -> dict:
    """GET with retry/backoff on rate limiting and the API's frequent transient 5xx errors.

    Cloudflare's generic error interstitial for 502/503 often carries a
    `Retry-After: 60` header that has nothing to do with the actual API's
    rate limit, so it's only honored for a real 429 (an explicit rate-limit
    signal from the API itself); other retryable statuses use our own
    capped exponential backoff instead.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=_headers(), timeout=30)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            wait = min(REQUEST_DELAY * (attempt + 1), 20)
            logger.warning(f"Request error for {url} {params}: {exc} (retrying in {wait:.0f}s)")
            time.sleep(wait)
            continue

        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", REQUEST_DELAY * (attempt + 1) * 2))
            logger.warning(f"429 from {url} {params}, retrying in {wait:.0f}s")
            time.sleep(wait)
            continue

        if resp.status_code in RETRYABLE_STATUS:
            wait = min(REQUEST_DELAY * (2 ** attempt), 20)
            logger.warning(f"{resp.status_code} from {url} {params}, retrying in {wait:.0f}s")
            time.sleep(wait)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError(f"Failed to fetch {url} {params} after {max_retries} retries") from last_exc


def _fetch_all_sets() -> list[dict]:
    sets: list[dict] = []
    page = 1
    while True:
        data = _request_json(SETS_URL, {"page": page, "pageSize": SETS_PAGE_SIZE})
        batch = data.get("data") or []
        sets.extend(batch)
        if len(batch) < SETS_PAGE_SIZE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return sets


def _fetch_set_cards(set_id: str) -> list[dict]:
    cards: list[dict] = []
    page = 1
    while True:
        data = _request_json(
            CARDS_URL,
            {"q": f"set.id:{set_id}", "page": page, "pageSize": CARDS_PAGE_SIZE, "orderBy": "number"},
        )
        batch = data.get("data") or []
        cards.extend(batch)
        if len(batch) < CARDS_PAGE_SIZE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)
    return cards


def _download_image(card_id: str, url: str) -> str:
    ext = Path(url.split("?")[0]).suffix or ".png"
    dest = IMAGES_DIR / f"{card_id}{ext}"
    if dest.exists() and dest.stat().st_size > 0:
        return dest.name
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return dest.name


def _extract_int(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    match = re.search(r"\d+", value)
    return int(match.group()) if match else None


# pokemontcg.io doesn't publish print-variant availability directly, but each
# card's tcgplayer pricing keys are effectively a per-card variant list (only
# keys for variants that were actually printed get a price entry at all).
def _variants_from_card(card: dict) -> list[str]:
    prices = ((card.get("tcgplayer") or {}).get("prices")) or {}
    variants: set[str] = set()
    for key in prices:
        kl = key.lower()
        if "reverse" in kl:
            base = "reverse_holo"
        elif "holo" in kl:
            base = "holo"
        else:
            base = "normal"
        # "1st Edition Holofoil" and "Unlimited Holofoil" are two distinct,
        # separately-collectible prints (common on vintage cards) — fold
        # first-edition into the variant name itself rather than adding it
        # as a second independent flag, which would otherwise make a single
        # "1st Edition Holo" print look like two unrelated checklist items.
        if "1st" in kl or "firstedition" in kl:
            variants.add(f"first_edition_{base}")
        else:
            variants.add(base)
    return sorted(variants)


def _card_to_row(card: dict, image_filename: Optional[str]) -> tuple:
    images = card.get("images") or {}
    image_url = images.get("large") or images.get("small")
    card_set = card.get("set") or {}
    number = card.get("number", "")
    hp = card.get("hp")
    dex_numbers = card.get("nationalPokedexNumbers") or []
    release_date = card_set.get("releaseDate")
    if release_date:
        release_date = release_date.replace("/", "-")
    return (
        card["id"],
        card.get("name"),
        card.get("supertype"),
        json.dumps(card.get("subtypes") or []),
        json.dumps(card.get("types") or []),
        hp,
        _extract_int(hp),
        card.get("rarity"),
        card_set.get("id"),
        card_set.get("name"),
        card_set.get("series"),
        release_date,
        number,
        _extract_int(number),
        card.get("artist"),
        dex_numbers[0] if dex_numbers else None,
        "en",
        json.dumps(_variants_from_card(card)),
        image_url,
        image_filename,
    )


def _sync_one_set(conn, pool: ThreadPoolExecutor, set_info: dict) -> int:
    cards = _fetch_set_cards(set_info["id"])
    if not cards:
        return 0

    futures = {}
    for card in cards:
        images = card.get("images") or {}
        image_url = images.get("large") or images.get("small")
        if image_url:
            futures[pool.submit(_download_image, card["id"], image_url)] = card

    filenames: dict[str, str] = {}
    for future in as_completed(futures):
        card = futures[future]
        try:
            filenames[card["id"]] = future.result()
            _sync_status["images_downloaded"] += 1
        except Exception as exc:
            logger.warning(f"Image download failed for {card['id']}: {exc}")

    rows = [_card_to_row(card, filenames.get(card["id"])) for card in cards]
    upsert_cards(conn, rows)
    return len(rows)


def sync_pokemon_data(new_only: bool = False) -> dict:
    """Walk every set in the Pokemon TCG API, upserting card metadata and
    downloading any card art not already cached on disk. With new_only, sets
    already in the database are skipped entirely (nothing existing is
    touched) and only sets not seen before are fetched."""
    if _sync_status["running"]:
        return {"started": False, "message": "sync already running"}

    _sync_status.update(
        running=True,
        new_only=new_only,
        phase="listing sets",
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
        sets = _fetch_all_sets()
        if new_only:
            known = existing_set_keys()
            sets = [s for s in sets if (s["id"], "en") not in known]
        _sync_status["total_sets"] = len(sets)
        _sync_status["total_cards"] = sum(s.get("total") or 0 for s in sets)
        _sync_status["phase"] = "syncing cards"

        with ThreadPoolExecutor(max_workers=IMAGE_DOWNLOAD_WORKERS) as pool:
            for set_info in sets:
                _sync_status["current_set"] = set_info.get("name")
                try:
                    synced = _sync_one_set(conn, pool, set_info)
                    _sync_status["cards_synced"] += synced
                except Exception as exc:
                    logger.warning(f"Skipping set {set_info.get('id')}: {exc}")
                    _sync_status["sets_failed"] += 1
                finally:
                    _sync_status["sets_done"] += 1
                time.sleep(REQUEST_DELAY)

        _sync_status["last_synced"] = datetime.now(timezone.utc).isoformat()
        log_sync("success", _sync_status["cards_synced"], _sync_status["images_downloaded"])
        return {
            "started": True,
            "cards_synced": _sync_status["cards_synced"],
            "images_downloaded": _sync_status["images_downloaded"],
            "sets_failed": _sync_status["sets_failed"],
        }

    except Exception as exc:
        logger.exception("Pokemon sync failed")
        _sync_status["last_error"] = str(exc)
        log_sync("error", _sync_status["cards_synced"], _sync_status["images_downloaded"], error=str(exc))
        raise

    finally:
        _sync_status["running"] = False
        _sync_status["phase"] = None
        _sync_status["current_set"] = None
        conn.close()


async def sync_pokemon_data_async(new_only: bool = False) -> dict:
    return await asyncio.to_thread(sync_pokemon_data, new_only)
