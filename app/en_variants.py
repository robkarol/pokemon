"""
Fill in print variants (normal / holo / reverse holo / 1st Edition) for
English cards from TCGdex.

pokemontcg.io has no variant field; pokemon_data.py infers variants from a
card's TCGplayer price keys, which are missing for new sets and often omit
reverse holos. TCGdex publishes explicit per-card variant flags. This maps
each English set/card from pokemontcg.io onto its TCGdex counterpart, stores
TCGdex's flags in variant_hints, and merges them into cards.variants (see
database.merge_variants for which source wins).
"""
import asyncio
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional

from app.database import get_connection, merge_variants
from app.tcgdex_data import API_BASE, _card_variants, _request_json

logger = logging.getLogger(__name__)

WORKERS = 8

# pokemontcg.io set ids whose TCGdex set matches neither by name nor by id.
SET_ALIASES = {"fut20": "fut2020"}

_status: dict = {
    "running": False,
    "current_set": None,
    "sets_done": 0,
    "total_sets": None,
    "cards_updated": 0,
    "last_synced": None,
    "last_error": None,
}


def get_status() -> dict:
    return dict(_status)


def _norm_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower().replace("&", "and"))


def _norm_number(number: str) -> str:
    """'063' and '63' -> '63', 'TG01' -> 'tg1', so card numbers line up
    between the two APIs despite different zero-padding."""
    n = (number or "").strip().lower()
    m = re.match(r"^([a-z\-]*?)0*(\d+)([a-z]*)$", n)
    return f"{m.group(1)}{int(m.group(2))}{m.group(3)}" if m else n


def _tcgdex_set_id(set_id: str, set_name: str, by_name: dict, ids: set) -> Optional[str]:
    return by_name.get(_norm_name(set_name)) or (set_id if set_id in ids else None) or SET_ALIASES.get(set_id)


def backfill_en_variants(set_ids: Optional[list[str]] = None, only_missing: bool = False) -> dict:
    """Refresh TCGdex variant hints for English sets. set_ids limits it to
    those sets; only_missing limits it to sets with a card that has no hint
    yet (e.g. after a resync added cards)."""
    if _status["running"]:
        return {"started": False, "message": "already running"}
    _status.update(running=True, current_set=None, sets_done=0, total_sets=None,
                   cards_updated=0, last_error=None)
    conn = get_connection()
    try:
        tcg_sets = _request_json(f"{API_BASE}/en/sets") or []
        by_name = {_norm_name(s["name"]): s["id"] for s in tcg_sets}
        tcg_ids = {s["id"] for s in tcg_sets}

        query = "SELECT set_id, MIN(set_name) AS set_name FROM cards WHERE language = 'en' AND set_id IS NOT NULL"
        if only_missing:
            query += " AND id NOT IN (SELECT card_id FROM variant_hints)"
        sets = conn.execute(query + " GROUP BY set_id").fetchall()
        if set_ids is not None:
            sets = [s for s in sets if s["set_id"] in set(set_ids)]
        _status["total_sets"] = len(sets)

        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for s in sets:
                _status["current_set"] = s["set_name"]
                try:
                    _backfill_set(conn, pool, s["set_id"], _tcgdex_set_id(s["set_id"], s["set_name"], by_name, tcg_ids))
                except Exception as exc:
                    logger.warning(f"Variant backfill failed for {s['set_id']}: {exc}")
                finally:
                    _status["sets_done"] += 1

        _status["last_synced"] = datetime.now(timezone.utc).isoformat()
        return {"started": True, "cards_updated": _status["cards_updated"]}
    except Exception as exc:
        logger.exception("Variant backfill failed")
        _status["last_error"] = str(exc)
        raise
    finally:
        _status["running"] = False
        _status["current_set"] = None
        conn.close()


def _backfill_set(conn, pool: ThreadPoolExecutor, set_id: str, tcg_set_id: Optional[str]) -> None:
    if not tcg_set_id:
        logger.info(f"No TCGdex match for English set {set_id}; skipping variants")
        return
    detail = _request_json(f"{API_BASE}/en/sets/{tcg_set_id}") or {}
    local = {_norm_number(c["localId"]): c["id"] for c in detail.get("cards") or []}
    cards = conn.execute(
        "SELECT id, number, variants FROM cards WHERE language = 'en' AND set_id = ?", (set_id,)
    ).fetchall()
    matched = [(c, local[_norm_number(c["number"])]) for c in cards if _norm_number(c["number"]) in local]

    fetched = pool.map(lambda pair: _request_json(f"{API_BASE}/en/cards/{pair[1]}"), matched)
    for (card, _), tcg_card in zip(matched, fetched):
        if not tcg_card:
            continue
        hints = _card_variants(tcg_card)
        conn.execute(
            """
            INSERT INTO variant_hints (card_id, variants) VALUES (?, ?)
            ON CONFLICT(card_id) DO UPDATE SET variants = excluded.variants, fetched_at = CURRENT_TIMESTAMP
            """,
            (card["id"], json.dumps(hints)),
        )
        current = json.loads(card["variants"] or "[]")
        merged = merge_variants(current, hints)
        if merged != sorted(current):
            conn.execute("UPDATE cards SET variants = ? WHERE id = ?", (json.dumps(merged), card["id"]))
            _status["cards_updated"] += 1
    conn.commit()


async def backfill_en_variants_async(set_ids: Optional[list[str]] = None, only_missing: bool = False) -> dict:
    return await asyncio.to_thread(backfill_en_variants, set_ids, only_missing)
