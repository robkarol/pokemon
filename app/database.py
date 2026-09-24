"""
SQLite database for caching Pokemon TCG card metadata and tracking a
personal collection of owned cards.
"""
import json
import re
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path("/app/data/pokemon.db")

# sort key -> ordered list of sort terms. A plain string gets the caller's
# chosen direction; a (expr, fixed_direction) tuple always sorts that way
# regardless of it — used so "pokedex_number IS NULL" always pushes cards
# with no pokedex number to the bottom, in both ascending and descending order.
SORT_COLUMNS = {
    "name": ["name COLLATE NOCASE"],
    "set": ["set_release_date", "number_sort", "name COLLATE NOCASE"],
    "number": ["number_sort", "name COLLATE NOCASE"],
    "rarity": ["rarity COLLATE NOCASE", "name COLLATE NOCASE"],
    "hp": ["hp_sort", "name COLLATE NOCASE"],
    "pokedex": [("pokedex_number IS NULL", "ASC"), "pokedex_number", "name COLLATE NOCASE"],
}

# Shared by every sync source (pokemontcg.io, TCGdex, ...) so the row shape
# and upsert behavior stay identical regardless of where a card came from.
CARD_COLUMNS = [
    "id", "name", "supertype", "subtypes", "types", "hp", "hp_sort", "rarity",
    "set_id", "set_name", "series", "set_release_date", "number", "number_sort",
    "artist", "pokedex_number", "language", "variants", "image_url", "image_filename",
]

_UPSERT_CARD_SQL = f"""
    INSERT INTO cards ({", ".join(CARD_COLUMNS)}, synced_at)
    VALUES ({", ".join("?" for _ in CARD_COLUMNS)}, CURRENT_TIMESTAMP)
    ON CONFLICT(id) DO UPDATE SET
        {", ".join(f"{c}=excluded.{c}" for c in CARD_COLUMNS if c not in ("id", "image_filename"))},
        image_filename=COALESCE(excluded.image_filename, cards.image_filename),
        synced_at=CURRENT_TIMESTAMP
"""


# Variant strings come from each card's own `variants` field (see
# pokemon_data.py / tcgdex_data.py), not a fixed enum here, so the print
# types a binder or the collection can track grow with the data instead of
# being capped at "normal" and "reverse_holo". This just guards against
# garbage input reaching the column.
_VARIANT_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{0,39}$")


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
            variants TEXT,
            image_url TEXT,
            image_filename TEXT,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS collection (
            user_id TEXT NOT NULL DEFAULT '',
            card_id TEXT NOT NULL REFERENCES cards(id),
            variant TEXT NOT NULL DEFAULT 'normal',
            quantity INTEGER NOT NULL DEFAULT 1,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, card_id, variant)
        );

        CREATE TABLE IF NOT EXISTS wishlist (
            user_id TEXT NOT NULL,
            card_id TEXT NOT NULL REFERENCES cards(id),
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, card_id)
        );

        CREATE TABLE IF NOT EXISTS sync_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT,
            cards_synced INTEGER,
            images_downloaded INTEGER,
            error TEXT,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS binders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL DEFAULT '',
            name TEXT NOT NULL,
            color TEXT NOT NULL DEFAULT '#ffcb05',
            page_count INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS binder_slots (
            binder_id INTEGER NOT NULL REFERENCES binders(id),
            page_number INTEGER NOT NULL,
            slot_index INTEGER NOT NULL,
            card_id TEXT NOT NULL REFERENCES cards(id),
            variant TEXT NOT NULL DEFAULT 'normal',
            PRIMARY KEY (binder_id, page_number, slot_index)
        );
        CREATE INDEX IF NOT EXISTS idx_binder_slots_binder ON binder_slots(binder_id);
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
    if "variants" not in cols:
        conn.execute("ALTER TABLE cards ADD COLUMN variants TEXT")
    conn.commit()

    binder_cols = {row[1] for row in conn.execute("PRAGMA table_info(binders)").fetchall()}
    if binder_cols and "color" not in binder_cols:
        conn.execute("ALTER TABLE binders ADD COLUMN color TEXT NOT NULL DEFAULT '#ffcb05'")
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

    # Migration: accounts. Rows that predate them get user_id '' ("unowned")
    # and are handed to a real account later via claim_legacy_data().
    # The collection PK must now include user_id, which SQLite can only do
    # by rebuilding the table.
    collection_cols = {row[1] for row in conn.execute("PRAGMA table_info(collection)").fetchall()}
    if "user_id" not in collection_cols:
        conn.executescript("""
            ALTER TABLE collection RENAME TO collection_old;
            CREATE TABLE collection (
                user_id TEXT NOT NULL DEFAULT '',
                card_id TEXT NOT NULL REFERENCES cards(id),
                variant TEXT NOT NULL DEFAULT 'normal',
                quantity INTEGER NOT NULL DEFAULT 1,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, card_id, variant)
            );
            INSERT INTO collection (user_id, card_id, variant, quantity, added_at)
                SELECT '', card_id, variant, quantity, added_at FROM collection_old;
            DROP TABLE collection_old;
        """)
        conn.commit()
    binder_cols = {row[1] for row in conn.execute("PRAGMA table_info(binders)").fetchall()}
    if "user_id" not in binder_cols:
        conn.execute("ALTER TABLE binders ADD COLUMN user_id TEXT NOT NULL DEFAULT ''")
        conn.commit()

    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_binders_user    ON binders(user_id);
        CREATE INDEX IF NOT EXISTS idx_wishlist_user   ON wishlist(user_id);
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


def get_owned_facets(user_id: str) -> dict:
    """Facet values scoped to one user's collection, for the binder picker's
    filters — no point offering a set/rarity/type they don't own."""
    conn = get_connection()
    try:
        base = "FROM collection JOIN cards ON cards.id = collection.card_id"
        scope = "collection.user_id = ?"
        sets = conn.execute(f"""
            SELECT DISTINCT cards.set_id AS id, cards.set_name AS name
            {base}
            WHERE {scope} AND cards.set_id IS NOT NULL
            ORDER BY cards.set_name COLLATE NOCASE
        """, (user_id,)).fetchall()
        rarities = conn.execute(f"""
            SELECT DISTINCT cards.rarity AS rarity {base}
            WHERE {scope} AND cards.rarity IS NOT NULL AND cards.rarity != ''
            ORDER BY cards.rarity
        """, (user_id,)).fetchall()
        supertypes = conn.execute(f"""
            SELECT DISTINCT cards.supertype AS supertype {base}
            WHERE {scope} AND cards.supertype IS NOT NULL AND cards.supertype != ''
            ORDER BY cards.supertype
        """, (user_id,)).fetchall()
        languages = conn.execute(f"""
            SELECT DISTINCT cards.language AS language {base}
            WHERE {scope} AND cards.language IS NOT NULL AND cards.language != ''
            ORDER BY cards.language
        """, (user_id,)).fetchall()
        type_rows = conn.execute(f"""
            SELECT DISTINCT cards.types AS types {base}
            WHERE {scope} AND cards.types IS NOT NULL AND cards.types != '[]'
        """, (user_id,)).fetchall()

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


def _attach_owned_breakdown(conn: sqlite3.Connection, user_id: str, rows: list) -> list[dict]:
    """Merge this user's per-variant owned quantity in as owned_by_variant,
    plus owned_quantity as their sum — however many distinct variants a
    card actually has — and a `wishlisted` flag."""
    items = [dict(r) for r in rows]
    if not items:
        return items
    card_ids = [item["id"] for item in items]
    placeholders = ",".join("?" for _ in card_ids)
    collection_rows = conn.execute(
        f"""
        SELECT card_id, variant, SUM(quantity) AS qty FROM collection
        WHERE user_id = ? AND card_id IN ({placeholders}) GROUP BY card_id, variant
        """,
        [user_id, *card_ids],
    ).fetchall()
    by_card: dict[str, dict[str, int]] = {}
    for r in collection_rows:
        by_card.setdefault(r["card_id"], {})[r["variant"]] = r["qty"]
    wished = {
        r["card_id"]
        for r in conn.execute(
            f"SELECT card_id FROM wishlist WHERE user_id = ? AND card_id IN ({placeholders})",
            [user_id, *card_ids],
        ).fetchall()
    }
    for item in items:
        breakdown = by_card.get(item["id"], {})
        item["owned_by_variant"] = breakdown
        item["owned_quantity"] = sum(breakdown.values())
        item["wishlisted"] = item["id"] in wished
    return items


def query_cards(
    user_id: str,
    search: str = "",
    set_id: str = "",
    rarity: str = "",
    supertype: str = "",
    card_type: str = "",
    language: str = "",
    has_variant: str = "",
    owned: bool = False,
    wishlist: bool = False,
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
        if has_variant:
            clauses.append("cards.variants LIKE ?")
            params.append(f'%"{has_variant}"%')

        if owned:
            clauses.append(
                "cards.id IN (SELECT card_id FROM collection WHERE user_id = ? "
                "GROUP BY card_id HAVING SUM(quantity) > 0)"
            )
            params.append(user_id)
        if wishlist:
            clauses.append("cards.id IN (SELECT card_id FROM wishlist WHERE user_id = ?)")
            params.append(user_id)

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        columns = SORT_COLUMNS.get(sort, SORT_COLUMNS["set"])
        direction = "DESC" if order == "desc" else "ASC"
        order_terms = [
            f"{col[0]} {col[1]}" if isinstance(col, tuple) else f"{col} {direction}"
            for col in columns
        ]
        order_clause = "ORDER BY " + ", ".join(order_terms)

        total = conn.execute(
            f"SELECT COUNT(*) AS c FROM cards {where}",
            params,
        ).fetchone()["c"]

        page = max(page, 1)
        page_size = min(max(page_size, 1), 120)
        offset = (page - 1) * page_size

        rows = conn.execute(
            f"""
            SELECT cards.*
            FROM cards {where} {order_clause} LIMIT ? OFFSET ?
            """,
            params + [page_size, offset],
        ).fetchall()

        return {
            "items": _attach_owned_breakdown(conn, user_id, rows),
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max((total + page_size - 1) // page_size, 1),
        }
    finally:
        conn.close()


def get_master_set(user_id: str, set_id: str) -> Optional[dict]:
    """Every card in one set, ordered by number, with its variants and
    current collection state — the checklist view for chasing a full
    master set (every card in every print variant it actually exists in)."""
    conn = get_connection()
    try:
        meta = conn.execute(
            "SELECT set_id AS id, set_name AS name, series, set_release_date AS release_date, language "
            "FROM cards WHERE set_id = ? LIMIT 1",
            (set_id,),
        ).fetchone()
        if not meta:
            return None

        rows = conn.execute(
            """
            SELECT cards.* FROM cards
            WHERE cards.set_id = ?
            ORDER BY number_sort IS NULL, number_sort, name COLLATE NOCASE
            """,
            (set_id,),
        ).fetchall()

        return {"set": dict(meta), "cards": _attach_owned_breakdown(conn, user_id, rows)}
    finally:
        conn.close()


def set_collection_quantity(user_id: str, card_id: str, variant: str, quantity: int) -> int:
    """Set (or clear, if quantity <= 0) how many copies of a card/variant the user owns. Returns the stored quantity."""
    if not _VARIANT_SLUG_RE.match(variant):
        raise ValueError(f"invalid variant {variant!r}")
    conn = get_connection()
    try:
        if quantity <= 0:
            conn.execute(
                "DELETE FROM collection WHERE user_id = ? AND card_id = ? AND variant = ?",
                (user_id, card_id, variant),
            )
            conn.commit()
            return 0
        conn.execute(
            """
            INSERT INTO collection (user_id, card_id, variant, quantity) VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, card_id, variant) DO UPDATE SET quantity = excluded.quantity
            """,
            (user_id, card_id, variant, quantity),
        )
        conn.commit()
        return quantity
    finally:
        conn.close()


def set_wishlisted(user_id: str, card_id: str, wanted: bool) -> bool:
    """Add/remove a card from the user's wishlist. Returns False if the card doesn't exist."""
    conn = get_connection()
    try:
        if wanted:
            if not conn.execute("SELECT 1 FROM cards WHERE id = ?", (card_id,)).fetchone():
                return False
            conn.execute(
                "INSERT OR IGNORE INTO wishlist (user_id, card_id) VALUES (?, ?)", (user_id, card_id)
            )
        else:
            conn.execute("DELETE FROM wishlist WHERE user_id = ? AND card_id = ?", (user_id, card_id))
        conn.commit()
        return True
    finally:
        conn.close()


def legacy_summary() -> dict:
    """Rows from before accounts existed (user_id ''), waiting to be claimed."""
    conn = get_connection()
    try:
        return {
            "collection_rows": conn.execute(
                "SELECT COUNT(*) AS c FROM collection WHERE user_id = ''"
            ).fetchone()["c"],
            "binders": conn.execute(
                "SELECT COUNT(*) AS c FROM binders WHERE user_id = ''"
            ).fetchone()["c"],
        }
    finally:
        conn.close()


def claim_legacy_data(user_id: str) -> dict:
    """Hand all pre-accounts collection rows and binders to one account.
    Collection quantities are merged into anything the account already owns."""
    if not user_id:
        raise ValueError("user_id required")
    conn = get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        moved = conn.execute("SELECT COUNT(*) AS c FROM collection WHERE user_id = ''").fetchone()["c"]
        conn.execute(
            """
            INSERT INTO collection (user_id, card_id, variant, quantity, added_at)
            SELECT ?, card_id, variant, quantity, added_at FROM collection WHERE user_id = ''
            ON CONFLICT(user_id, card_id, variant) DO UPDATE SET quantity = quantity + excluded.quantity
            """,
            (user_id,),
        )
        conn.execute("DELETE FROM collection WHERE user_id = ''")
        binders = conn.execute("UPDATE binders SET user_id = ? WHERE user_id = ''", (user_id,)).rowcount
        conn.commit()
        return {"collection_rows": moved, "binders": binders}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def collection_summary(user_id: str) -> dict:
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(DISTINCT card_id) AS distinct_cards, COALESCE(SUM(quantity), 0) AS total_copies "
            "FROM collection WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def list_owned_cards(
    user_id: str,
    search: str = "",
    set_id: str = "",
    rarity: str = "",
    supertype: str = "",
    card_type: str = "",
    language: str = "",
) -> list[dict]:
    """Owned (card, variant) pairs still free to place in a binder. A copy
    already sitting in any binder slot (any binder, not just the current
    one — you only have the one physical card) counts against the owned
    quantity, so a card drops out of the picker once every copy has a home."""
    conn = get_connection()
    try:
        clauses = ["collection.user_id = ?", "(collection.quantity - COALESCE(placed.placed_count, 0)) > 0"]
        params: list = [user_id]
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
        where = "WHERE " + " AND ".join(clauses)
        rows = conn.execute(
            f"""
            SELECT cards.id, cards.name, cards.image_filename, cards.set_name, cards.number,
                   cards.rarity, collection.variant,
                   (collection.quantity - COALESCE(placed.placed_count, 0)) AS quantity
            FROM collection
            JOIN cards ON cards.id = collection.card_id
            LEFT JOIN (
                SELECT bs.card_id, bs.variant, COUNT(*) AS placed_count
                FROM binder_slots bs JOIN binders b ON b.id = bs.binder_id
                WHERE b.user_id = ?
                GROUP BY bs.card_id, bs.variant
            ) AS placed ON placed.card_id = collection.card_id AND placed.variant = collection.variant
            {where}
            ORDER BY cards.name COLLATE NOCASE, collection.variant
            """,
            [user_id] + params,
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Binders: a virtual card binder is a named grid of pages, each with a fixed
# number of pockets. Pockets only get a row here once a card is placed in
# them (empty pockets are implicit — the UI just knows page_count * SLOTS_PER_PAGE).
# ---------------------------------------------------------------------------

SLOTS_PER_PAGE = 9  # standard 3x3 "9-pocket" binder page


# A curated palette rather than a free color picker — keeps every binder
# cover looking intentional against the dark theme instead of clashing.
BINDER_COLORS = (
    "#ffcb05",  # yellow (site accent, default)
    "#ef4444",  # red
    "#fb923c",  # orange
    "#22c55e",  # green
    "#38bdf8",  # sky blue
    "#6366f1",  # indigo
    "#a78bfa",  # purple
    "#ec4899",  # pink
    "#94a3b8",  # slate / neutral
)
_HEX_COLOR_RE = re.compile(r"^#[0-9a-fA-F]{6}$")


def binder_owned_by(user_id: str, binder_id: int) -> bool:
    conn = get_connection()
    try:
        return conn.execute(
            "SELECT 1 FROM binders WHERE id = ? AND user_id = ?", (binder_id, user_id)
        ).fetchone() is not None
    finally:
        conn.close()


def list_binders(user_id: str) -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute("""
            SELECT b.id, b.name, b.color, b.page_count, b.created_at, COUNT(bs.card_id) AS filled_count
            FROM binders b LEFT JOIN binder_slots bs ON bs.binder_id = b.id
            WHERE b.user_id = ?
            GROUP BY b.id
            ORDER BY b.created_at DESC
        """, (user_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def create_binder(user_id: str, name: str, color: str = BINDER_COLORS[0]) -> dict:
    if not _HEX_COLOR_RE.match(color):
        raise ValueError(f"invalid color {color!r}")
    conn = get_connection()
    try:
        cursor = conn.execute(
            "INSERT INTO binders (user_id, name, color) VALUES (?, ?, ?)", (user_id, name, color)
        )
        conn.commit()
        return {"id": cursor.lastrowid, "name": name, "color": color, "page_count": 1, "filled_count": 0}
    finally:
        conn.close()


def update_binder(user_id: str, binder_id: int, name: Optional[str] = None, color: Optional[str] = None) -> Optional[dict]:
    if color is not None and not _HEX_COLOR_RE.match(color):
        raise ValueError(f"invalid color {color!r}")
    conn = get_connection()
    try:
        if not conn.execute(
            "SELECT 1 FROM binders WHERE id = ? AND user_id = ?", (binder_id, user_id)
        ).fetchone():
            return None
        if name is not None:
            conn.execute("UPDATE binders SET name = ? WHERE id = ?", (name, binder_id))
        if color is not None:
            conn.execute("UPDATE binders SET color = ? WHERE id = ?", (color, binder_id))
        conn.commit()
        row = conn.execute(
            """
            SELECT b.id, b.name, b.color, b.page_count, b.created_at, COUNT(bs.card_id) AS filled_count
            FROM binders b LEFT JOIN binder_slots bs ON bs.binder_id = b.id
            WHERE b.id = ? GROUP BY b.id
            """,
            (binder_id,),
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def delete_binder(user_id: str, binder_id: int) -> None:
    conn = get_connection()
    try:
        if not conn.execute(
            "SELECT 1 FROM binders WHERE id = ? AND user_id = ?", (binder_id, user_id)
        ).fetchone():
            return
        conn.execute("DELETE FROM binder_slots WHERE binder_id = ?", (binder_id,))
        conn.execute("DELETE FROM binders WHERE id = ?", (binder_id,))
        conn.commit()
    finally:
        conn.close()


def get_binder(user_id: str, binder_id: int) -> Optional[dict]:
    conn = get_connection()
    try:
        binder = conn.execute(
            "SELECT * FROM binders WHERE id = ? AND user_id = ?", (binder_id, user_id)
        ).fetchone()
        if not binder:
            return None
        rows = conn.execute(
            """
            SELECT bs.page_number, bs.slot_index, bs.variant, c.id AS card_id, c.name,
                   c.image_filename, c.set_name, c.number, c.rarity
            FROM binder_slots bs JOIN cards c ON c.id = bs.card_id
            WHERE bs.binder_id = ?
            """,
            (binder_id,),
        ).fetchall()
        slots = {f"{r['page_number']}:{r['slot_index']}": dict(r) for r in rows}
        return {"binder": dict(binder), "slots": slots}
    finally:
        conn.close()


def add_binder_page(binder_id: int) -> int:
    conn = get_connection()
    try:
        conn.execute("UPDATE binders SET page_count = page_count + 1 WHERE id = ?", (binder_id,))
        conn.commit()
        row = conn.execute("SELECT page_count FROM binders WHERE id = ?", (binder_id,)).fetchone()
        return row["page_count"] if row else 0
    finally:
        conn.close()


def remove_last_binder_page(binder_id: int) -> dict:
    """Removes the last page (and any cards placed in it). Refuses to go below 1 page."""
    conn = get_connection()
    try:
        row = conn.execute("SELECT page_count FROM binders WHERE id = ?", (binder_id,)).fetchone()
        if not row:
            return {"removed": False, "reason": "binder not found"}
        if row["page_count"] <= 1:
            return {"removed": False, "reason": "a binder must have at least one page"}
        last_page = row["page_count"]
        conn.execute(
            "DELETE FROM binder_slots WHERE binder_id = ? AND page_number = ?", (binder_id, last_page)
        )
        conn.execute("UPDATE binders SET page_count = page_count - 1 WHERE id = ?", (binder_id,))
        conn.commit()
        return {"removed": True, "page_count": last_page - 1}
    finally:
        conn.close()


def set_binder_slot(binder_id: int, page: int, index: int, card_id: str, variant: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO binder_slots (binder_id, page_number, slot_index, card_id, variant)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(binder_id, page_number, slot_index)
                DO UPDATE SET card_id = excluded.card_id, variant = excluded.variant
            """,
            (binder_id, page, index, card_id, variant),
        )
        conn.commit()
    finally:
        conn.close()


def clear_binder_slot(binder_id: int, page: int, index: int) -> None:
    conn = get_connection()
    try:
        conn.execute(
            "DELETE FROM binder_slots WHERE binder_id = ? AND page_number = ? AND slot_index = ?",
            (binder_id, page, index),
        )
        conn.commit()
    finally:
        conn.close()
