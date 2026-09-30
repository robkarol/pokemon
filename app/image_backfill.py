"""
Find card art for Japanese/Thai cards that TCGdex's API lists without an
image. Two fallbacks, tried in order:

1. TCGdex's own asset CDN. It often has the scan even when the API's card
   record has no `image` field. Same language as the card.
2. Limitless TCG's Japanese card scans (Japanese cards only). Limitless
   uses the same set codes as TCGdex minus dashes (SV-P -> SVP) and
   unpadded card numbers.

3. The official Thai card database (asia.pokemon-card.com/th), Thai cards
   only. It filters by the same set codes (an unknown code lists nothing);
   each card's detail page gives its collector number, which is how its scan
   is matched to our card.

Limitless is not used for Thai cards: it has Japanese scans for the same set
codes, but those would show Japanese text on a Thai card.
"""
import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

import requests

try:  # optional: only used to shrink the official Thai site's huge scans
    from PIL import Image
except ImportError:
    Image = None

from app.database import get_connection
from app.tcgdex_data import API_BASE, IMAGES_DIR, _request_json

logger = logging.getLogger(__name__)

WORKERS = 8
TCGDEX_ASSETS = "https://assets.tcgdex.net"
LIMITLESS_JP = "https://limitlesstcg.nyc3.cdn.digitaloceanspaces.com/tpc"
ASIA_TH = "https://asia.pokemon-card.com/th/card-search"
ASIA_WORKERS = 4  # be gentle with the official site
_BROWSER_HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_WIDTH = 734  # the official Thai scans are 2480px wide (~2 MB each)

_status: dict = {
    "running": False,
    "current_set": None,
    "sets_done": 0,
    "total_sets": None,
    "images_found": 0,
    "last_synced": None,
    "last_error": None,
}


def get_status() -> dict:
    return dict(_status)


def _candidate_urls(lang: str, serie_id: Optional[str], tcg_set_id: str, number: str) -> list[str]:
    urls = []
    if serie_id:
        urls.append(f"{TCGDEX_ASSETS}/{lang}/{serie_id}/{tcg_set_id}/{number}/high.png")
    if lang == "ja":
        code = tcg_set_id.replace("-", "")
        num = re.sub(r"^0+(?=\d)", "", number)
        urls.append(f"{LIMITLESS_JP}/{code}/{code}_{num}_R_JP_LG.png")
    return urls


def _fetch_first(card_id: str, urls: list[str]) -> Optional[tuple[str, str]]:
    """Download the first URL that serves an image; returns (url, filename)."""
    dest = IMAGES_DIR / f"{card_id}.png"
    for url in urls:
        try:
            resp = requests.get(url, timeout=30)
        except requests.RequestException:
            continue
        if (resp.status_code == 200 and resp.content
                and resp.headers.get("content-type", "").startswith("image/")):
            dest.write_bytes(resp.content)
            _shrink(dest)
            return url, dest.name
    return None


def _shrink(path) -> None:
    if Image is None:
        return
    try:
        with Image.open(path) as im:
            if im.width <= MAX_WIDTH:
                return
            im.thumbnail((MAX_WIDTH, MAX_WIDTH * 2), Image.LANCZOS)
            im.save(path, optimize=True)
    except Exception as exc:
        logger.warning(f"Could not resize {path}: {exc}")


def backfill_images(languages: tuple[str, ...] = ("ja", "th")) -> dict:
    if _status["running"]:
        return {"started": False, "message": "already running"}
    _status.update(running=True, current_set=None, sets_done=0, total_sets=None,
                   images_found=0, last_error=None)
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    try:
        placeholders = ",".join("?" for _ in languages)
        rows = conn.execute(
            f"""
            SELECT id, language, set_id, set_name, number FROM cards
            WHERE language IN ({placeholders}) AND image_filename IS NULL
            ORDER BY set_id
            """,
            languages,
        ).fetchall()
        by_set: dict[tuple[str, str], list] = {}
        for r in rows:
            by_set.setdefault((r["language"], r["set_id"]), []).append(r)
        _status["total_sets"] = len(by_set)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for (lang, set_id), cards in by_set.items():
                _status["current_set"] = cards[0]["set_name"]
                try:
                    _backfill_set(conn, pool, lang, set_id, cards)
                except Exception as exc:
                    logger.warning(f"Image backfill failed for {set_id}: {exc}")
                finally:
                    _status["sets_done"] += 1

        _status["last_synced"] = datetime.now(timezone.utc).isoformat()
        return {"started": True, "images_found": _status["images_found"]}
    except Exception as exc:
        logger.exception("Image backfill failed")
        _status["last_error"] = str(exc)
        raise
    finally:
        _status["running"] = False
        _status["current_set"] = None
        conn.close()


def _backfill_set(conn, pool: ThreadPoolExecutor, lang: str, set_id: str, cards: list) -> None:
    tcg_set_id = set_id.split("-", 2)[2]  # "tcgdex-ja-SV-P" -> "SV-P"
    detail = _request_json(f"{API_BASE}/{lang}/sets/{tcg_set_id}") or {}
    serie_id = (detail.get("serie") or {}).get("id")

    results = pool.map(
        lambda c: _fetch_first(c["id"], _candidate_urls(lang, serie_id, tcg_set_id, c["number"] or "")),
        cards,
    )
    still_missing = []
    for card, found in zip(cards, results):
        if found:
            _save(conn, card["id"], *found)
        else:
            still_missing.append(card)
    conn.commit()

    if lang == "th" and still_missing:
        official = _asia_th_images(tcg_set_id)
        wanted = []
        for card in still_missing:
            url = official.get(_int_number(card["number"]))
            if url:
                wanted.append((card, url))
        results = pool.map(lambda pair: _fetch_first(pair[0]["id"], [pair[1]]), wanted)
        for (card, _), found in zip(wanted, results):
            if found:
                _save(conn, card["id"], *found)
        conn.commit()


def _save(conn, card_id: str, url: str, filename: str) -> None:
    conn.execute("UPDATE cards SET image_url = ?, image_filename = ? WHERE id = ?", (url, filename, card_id))
    _status["images_found"] += 1


def _int_number(number: Optional[str]) -> Optional[int]:
    m = re.match(r"^\s*0*(\d+)", number or "")
    return int(m.group(1)) if m else None


def _get_html(url: str) -> str:
    resp = requests.get(url, headers=_BROWSER_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def _asia_th_images(set_code: str) -> dict[int, str]:
    """collector number -> scan URL for one set on the official Thai site."""
    detail_ids: list[str] = []
    page = 1
    while page <= 50:
        html = _get_html(f"{ASIA_TH}/list/?pageNo={page}&expansionCodes={set_code}")
        ids = list(dict.fromkeys(re.findall(r"/th/card-search/detail/(\d+)/", html)))
        detail_ids.extend(ids)
        if not ids or f"pageNo={page + 1}&" not in html:
            break
        page += 1

    def number_and_image(detail_id: str) -> Optional[tuple[int, str]]:
        try:
            html = _get_html(f"{ASIA_TH}/detail/{detail_id}/")
        except requests.RequestException:
            return None
        num = re.search(r'class="collectorNumber">\s*(\d+)\s*/', html)
        img = re.search(r"https://asia\.pokemon-card\.com/th/card-img/th\d+\.png", html)
        if not (num and img):
            return None
        return int(num.group(1)), img.group(0)

    with ThreadPoolExecutor(max_workers=ASIA_WORKERS) as pool:
        found = [r for r in pool.map(number_and_image, detail_ids) if r]
    return dict(found)


async def backfill_images_async() -> dict:
    return await asyncio.to_thread(backfill_images)
