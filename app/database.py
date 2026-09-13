"""
SQLite database for caching Pokemon TCG card metadata and tracking a
personal collection of owned cards.
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
    "pokedex": ["pokedex_number", "name COLLATE NOCASE"],
}

# Shared by every sync source (pokemontcg.io, TCGdex, ...) so the row shape
# and upsert behavior stay identical regardless of where a card came from.
CARD_COLUMNS = [
    "id", "name", "supertype", "subtypes", "types", "hp", "hp_sort", "rarity",
    "set_id", "set_name", "series", "set_release_date", "number", "number_sort",
    "artist", "pokedex_number", "language", "image_url", "image_filename",
]

_UPSERT_CARD_SQL = f"""
    INSERT INTO cards ({", ".join(CARD_COLUMNS)}, synced_at)
    VALUES ({", ".join("?" for _ in CARD_COLUMNS)}, CURRENT_TIMESTAMP)
    ON CONFLICT(id) DO UPDATE SET
        {", ".join(f"{c}=excluded.{c}" for c in CARD_COLUMNS if c not in ("id", "image_filename"))},
        image_filename=COALESCE(excluded.image_filename, cards.image_filename),
        synced_at=CURRENT_TIMESTAMP
"""


VARIANTS = ("normal", "reverse_holo")


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # WAL lets readers (e.g. the UI's /api/cards) proceed without blocking on
    # the background sync's frequent writes, which were otherwise the main
    # cause of slow/failed collection updates while a sync was running.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
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
            pokedex_number INTEGER,
            language TEXT NOT NULL DEFAULT 'en',
            image_url TEXT,
            image_filename TEXT,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS collection (
            card_id TEXT NOT NULL REFERENCES cards(id),
            variant TEXT NOT NULL DEFAULT 'normal',
            quantity INTEGER NOT NULL DEFAULT 1,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (card_id, variant)
        );

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

    # Migration: earlier deployments predate pokedex_number/language.
    # SQLite backfills a non-null DEFAULT into every existing row when a
    # column is added this way, so no separate UPDATE is needed.
    cols = {row[1] for row in conn.execute("PRAGMA table_info(cards)").fetchall()}
    if "pokedex_number" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN pokedex_number INTEGER")
    if "language" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN language TEXT NOT NULL DEFAULT 'en'")
    conn.commit()

    # Migration: the original collection table had no `variant` column and
    # a single-column (card_id) primary key. SQLite can't ALTER a primary
    # key in place, so rebuild the table, mapping existing rows to 'normal'.
    collection_cols = {row[1] for row in conn.execute("PRAGMA table_info(collection)").fetchall()}
    if collection_cols and "variant" not in collection_cols:
        conn.executescript("""
            ALTER TABLE collection RENAME TO collection_old;
            CREATE TABLE collection (
                card_id TEXT NOT NULL REFERENCES cards(id),
                variant TEXT NOT NULL DEFAULT 'normal',
                quantity INTEGER NOT NULL DEFAULT 1,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (card_id, variant)
            );
            INSERT INTO collection (card_id, variant, quantity, added_at)
                SELECT card_id, 'normal', quantity, added_at FROM collection_old;
            DROP TABLE collection_old;
        """)
        conn.commit()

    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_cards_name      ON cards(name COLLATE NOCASE);
        CREATE INDEX IF NOT EXISTS idx_cards_set       ON cards(set_id);
        CREATE INDEX IF NOT EXISTS idx_cards_rarity    ON cards(rarity);
        CREATE INDEX IF NOT EXISTS idx_cards_supertype ON cards(supertype);
        CREATE INDEX IF NOT EXISTS idx_cards_pokedex   ON cards(pokedex_number);
        CREATE INDEX IF NOT EXISTS idx_cards_language  ON cards(language);
    """)
    conn.commit()
    conn.close()


def has_data(language: Optional[str] = None) -> bool:
    conn = get_connection()
    try:
        if language:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM cards WHERE language = ?", (language,)
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) AS c FROM cards").fetchone()
        return row["c"] > 0
    finally:
        conn.close()


def upsert_cards(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    """Insert/update cards from any sync source. Each row must match CARD_COLUMNS order."""
    conn.executemany(_UPSERT_CARD_SQL, rows)
    conn.commit()


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
                   language, COUNT(*) AS card_count
            FROM cards
            WHERE set_id IS NOT NULL
            GROUP BY set_id, set_name, series, set_release_date, language
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
        languages = conn.execute("""
            SELECT DISTINCT language FROM cards
            WHERE language IS NOT NULL AND language != ''
            ORDER BY language
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
            "languages": [r["language"] for r in languages],
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
    language: str = "",
    owned: bool = False,
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
            clauses.append("cards.name LIKE ? COLLATE NOCASE")
            params.append(f"%{search}%")
        if set_id:
            clauses.append("cards.set_id = ?")
            params.append(set_id)
        if rarity:
            clauses.append("cards.rarity = ?")
            params.append(rarity)
        if supertype:
            clauses.append("cards.supertype = ?")
            params.append(supertype)
        if card_type:
            clauses.append("cards.types LIKE ?")
            params.append(f'%"{card_type}"%')
        if language:
            clauses.append("cards.language = ?")
            params.append(language)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        joined = "FROM cards LEFT JOIN collection ON collection.card_id = cards.id"
        group_having = "GROUP BY cards.id" + (
            " HAVING COALESCE(SUM(collection.quantity), 0) > 0" if owned else ""
        )

        columns = SORT_COLUMNS.get(sort, SORT_COLUMNS["set"])
        direction = "DESC" if order == "desc" else "ASC"
        order_clause = "ORDER BY " + ", ".join(f"{col} {direction}" for col in columns)

        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM (SELECT cards.id {joined} {where} {group_having}) sub",
            params,
        ).fetchone()["c"]

        page = max(page, 1)
        page_size = min(max(page_size, 1), 120)
        offset = (page - 1) * page_size

        rows = conn.execute(
            f"""
            SELECT cards.*,
                   COALESCE(SUM(collection.quantity), 0) AS owned_quantity,
                   COALESCE(SUM(CASE WHEN collection.variant = 'normal' THEN collection.quantity END), 0) AS owned_normal,
                   COALESCE(SUM(CASE WHEN collection.variant = 'reverse_holo' THEN collection.quantity END), 0) AS owned_reverse_holo
            {joined} {where} {group_having} {order_clause} LIMIT ? OFFSET ?
            """,
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


def set_collection_quantity(card_id: str, variant: str, quantity: int) -> int:
    """Set (or clear, if quantity <= 0) how many copies of a card/variant are owned. Returns the stored quantity."""
    if variant not in VARIANTS:
        raise ValueError(f"unknown variant {variant!r}")
    conn = get_connection()
    try:
        if quantity <= 0:
            conn.execute("DELETE FROM collection WHERE card_id = ? AND variant = ?", (card_id, variant))
            conn.commit()
            return 0
        conn.execute(
            """
            INSERT INTO collection (card_id, variant, quantity) VALUES (?, ?, ?)
            ON CONFLICT(card_id, variant) DO UPDATE SET quantity = excluded.quantity
            """,
            (card_id, variant, quantity),
        )
        conn.commit()
        return quantity
    finally:
        conn.close()


def collection_summary() -> dict:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT card_id) AS distinct_cards, COALESCE(SUM(quantity), 0) AS total_copies FROM collection"
        ).fetchone()
        return dict(row)
    finally:
        conn.close()
