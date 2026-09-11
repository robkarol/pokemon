"""
SQLite database for caching Pokemon TCG card metadata.
"""
import json
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path("/app/data/pokemon.db")

# sort key -> ordered list of safe column expressions (all get the same direction)
SORT_COLUMNS = {
    "name": ["name COLLATE NOCASE"],
    "set": ["set_release_date", "number_sort", "name COLLATE NOCASE"],
    "number": ["number_sort", "name COLLATE NOCASE"],
    "rarity": ["rarity COLLATE NOCASE", "name COLLATE NOCASE"],
    "hp": ["hp_sort", "name COLLATE NOCASE"],
}


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS cards (
            id TEXT PRIMARY KEY,
            name TEXT,
            supertype TEXT,
            subtypes TEXT,
            types TEXT,
            hp TEXT,
            hp_sort INTEGER,
            rarity TEXT,
            set_id TEXT,
            set_name TEXT,
            series TEXT,
            set_release_date TEXT,
            number TEXT,
            number_sort INTEGER,
            artist TEXT,
            image_url TEXT,
            image_filename TEXT,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_cards_name     ON cards(name COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_cards_set      ON cards(set_id);
        CREATE INDEX IF NOT EXISTS idx_cards_rarity   ON cards(rarity);
        CREATE INDEX IF NOT EXISTS idx_cards_supertype ON cards(supertype);

        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT,
            cards_synced INTEGER,
            images_downloaded INTEGER,
            error TEXT,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()


def has_data() -> bool:
    conn = get_connection()
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM cards").fetchone()
        return row["c"] > 0
    finally:
        conn.close()


def log_sync(status: str, cards_synced: int, images_downloaded: int, error: Optional[str] = None):
    conn = get_connection()
    conn.execute(
        "INSERT INTO sync_log (status, cards_synced, images_downloaded, error) VALUES (?, ?, ?, ?)",
        (status, cards_synced, images_downloaded, error),
    )
    conn.commit()
    conn.close()


def last_sync() -> Optional[sqlite3.Row]:
    conn = get_connection()
    try:
        return conn.execute(
            "SELECT * FROM sync_log ORDER BY synced_at DESC LIMIT 1"
        ).fetchone()
    finally:
        conn.close()


def get_facets() -> dict:
    conn = get_connection()
    try:
        sets = conn.execute("""
            SELECT set_id AS id, set_name AS name, series, set_release_date AS release_date,
                   COUNT(*) AS card_count
            FROM cards
            WHERE set_id IS NOT NULL
            GROUP BY set_id, set_name, series, set_release_date
            ORDER BY set_release_date IS NULL, set_release_date DESC, set_name
        """).fetchall()
        rarities = conn.execute("""
            SELECT DISTINCT rarity FROM cards
            WHERE rarity IS NOT NULL AND rarity != ''
            ORDER BY rarity
        """).fetchall()
        supertypes = conn.execute("""
            SELECT DISTINCT supertype FROM cards
            WHERE supertype IS NOT NULL AND supertype != ''
            ORDER BY supertype
        """).fetchall()
        type_rows = conn.execute("""
            SELECT DISTINCT types FROM cards WHERE types IS NOT NULL AND types != '[]'
        """).fetchall()

        types = set()
        for row in type_rows:
            try:
                for t in json.loads(row["types"]):
                    types.add(t)
            except (ValueError, TypeError):
                continue

        return {
            "sets": [dict(r) for r in sets],
            "rarities": [r["rarity"] for r in rarities],
            "supertypes": [r["supertype"] for r in supertypes],
            "types": sorted(types),
        }
    finally:
        conn.close()


def query_cards(
    search: str = "",
    set_id: str = "",
    rarity: str = "",
    supertype: str = "",
    card_type: str = "",
    sort: str = "set",
    order: str = "asc",
    page: int = 1,
    page_size: int = 60,
) -> dict:
    conn = get_connection()
    try:
        clauses = []
        params: list = []

        if search:
            clauses.append("name LIKE ? COLLATE NOCASE")
            params.append(f"%{search}%")
        if set_id:
            clauses.append("set_id = ?")
            params.append(set_id)
        if rarity:
            clauses.append("rarity = ?")
            params.append(rarity)
        if supertype:
            clauses.append("supertype = ?")
            params.append(supertype)
        if card_type:
            clauses.append("types LIKE ?")
            params.append(f'%"{card_type}"%')

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        columns = SORT_COLUMNS.get(sort, SORT_COLUMNS["set"])
        direction = "DESC" if order == "desc" else "ASC"
        order_clause = "ORDER BY " + ", ".join(f"{col} {direction}" for col in columns)

        total = conn.execute(f"SELECT COUNT(*) AS c FROM cards {where}", params).fetchone()["c"]

        page = max(page, 1)
        page_size = min(max(page_size, 1), 120)
        offset = (page - 1) * page_size

        rows = conn.execute(
            f"SELECT * FROM cards {where} {order_clause} LIMIT ? OFFSET ?",
            params + [page_size, offset],
        ).fetchall()

        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max((total + page_size - 1) // page_size, 1),
        }
    finally:
        conn.close()
