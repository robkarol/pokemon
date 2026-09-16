"""
Searchable, sortable gallery of Pokemon TCG card art, backed by a local
SQLite metadata cache and a flat folder of downloaded card images.
"""
import asyncio
import logging

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import (
    get_facets, get_master_set, has_data, init_db, query_cards,
    set_collection_quantity, collection_summary,
)
from app.pokemon_data import IMAGES_DIR, sync_pokemon_data_async
from app.pokemon_data import get_sync_status as get_pokemontcg_sync_status
from app.tcgdex_data import sync_tcgdex_data_async
from app.tcgdex_data import get_sync_status as get_tcgdex_sync_status

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGES_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Pokemon Card Gallery")
templates = Jinja2Templates(directory="app/templates")
app.mount("/images", StaticFiles(directory=str(IMAGES_DIR)), name="images")


@app.on_event("startup")
async def startup_event():
    init_db()

    if not has_data(language="en"):
        logger.info("No English cards cached yet — triggering initial sync in background")
        asyncio.create_task(sync_pokemon_data_async())


@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse(url="/cards")


@app.get("/cards")
async def cards_page(request: Request):
    return templates.TemplateResponse(request, "cards.html", {})


@app.get("/api/cards")
def api_cards(
    search: str = "",
    set_id: str = Query("", alias="set"),
    rarity: str = "",
    supertype: str = "",
    type: str = "",
    language: str = "",
    has_variant: str = "",
    owned: bool = False,
    sort: str = "set",
    order: str = "asc",
    page: int = 1,
    page_size: int = 60,
):
    # Plain `def`: FastAPI runs this in its worker threadpool instead of the
    # asyncio event loop, so a SQLite call that's briefly blocked behind the
    # background sync's writes doesn't stall every other request too.
    return query_cards(
        search=search.strip(),
        set_id=set_id,
        rarity=rarity,
        supertype=supertype,
        card_type=type,
        language=language,
        has_variant=has_variant,
        owned=owned,
        sort=sort,
        order=order,
        page=page,
        page_size=page_size,
    )


@app.get("/api/facets")
def api_facets():
    return get_facets()


@app.get("/master/{set_id}")
async def master_set_page(request: Request, set_id: str):
    return templates.TemplateResponse(request, "master_set.html", {})


@app.get("/api/sets/{set_id}/master")
def api_master_set(set_id: str):
    result = get_master_set(set_id)
    if result is None:
        raise HTTPException(status_code=404, detail="set not found")
    return result


@app.get("/api/sync-status")
def api_sync_status():
    # has_data is checked fresh against the DB (not just the in-memory
    # last_synced flag) so a restart doesn't make an already-populated
    # cache look unsynced until another sync happens to run.
    pokemontcg_status = get_pokemontcg_sync_status()
    pokemontcg_status["has_data"] = has_data(language="en")
    return {
        "pokemontcg": pokemontcg_status,
        "tcgdex": get_tcgdex_sync_status(),
    }


@app.post("/api/sync")
async def api_sync(source: str = "pokemontcg"):
    if source == "tcgdex":
        status = get_tcgdex_sync_status()
        if status["running"]:
            return {"started": False, "message": "sync already running"}
        asyncio.create_task(sync_tcgdex_data_async())
        return {"started": True}

    status = get_pokemontcg_sync_status()
    if status["running"]:
        return {"started": False, "message": "sync already running"}
    asyncio.create_task(sync_pokemon_data_async())
    return {"started": True}


@app.get("/api/collection")
def api_collection_summary():
    return collection_summary()


@app.put("/api/collection/{card_id}")
def api_set_collection(
    card_id: str,
    quantity: int = Query(..., ge=0, le=999),
    variant: str = Query("normal", pattern="^(normal|reverse_holo)$"),
):
    stored = set_collection_quantity(card_id, variant, quantity)
    return {"card_id": card_id, "variant": variant, "quantity": stored}


@app.delete("/api/collection/{card_id}")
def api_remove_collection(card_id: str, variant: str = Query("normal", pattern="^(normal|reverse_holo)$")):
    set_collection_quantity(card_id, variant, 0)
    return {"card_id": card_id, "variant": variant, "quantity": 0}
