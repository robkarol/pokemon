"""
Searchable, sortable gallery of Pokemon TCG card art, backed by a local
SQLite metadata cache and a flat folder of downloaded card images.
"""
import asyncio
import logging

from fastapi import FastAPI, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import get_facets, has_data, init_db, query_cards
from app.pokemon_data import IMAGES_DIR, get_sync_status, sync_pokemon_data_async

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IMAGES_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Pokemon Card Gallery")
templates = Jinja2Templates(directory="app/templates")
app.mount("/images", StaticFiles(directory=str(IMAGES_DIR)), name="images")


@app.on_event("startup")
async def startup_event():
    init_db()

    if not has_data():
        logger.info("No cards cached yet — triggering initial sync in background")
        asyncio.create_task(sync_pokemon_data_async())


@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse(url="/cards")


@app.get("/cards")
async def cards_page(request: Request):
    return templates.TemplateResponse(request, "cards.html", {})


@app.get("/api/cards")
async def api_cards(
    search: str = "",
    set_id: str = Query("", alias="set"),
    rarity: str = "",
    supertype: str = "",
    type: str = "",
    sort: str = "set",
    order: str = "asc",
    page: int = 1,
    page_size: int = 60,
):
    return query_cards(
        search=search.strip(),
        set_id=set_id,
        rarity=rarity,
        supertype=supertype,
        card_type=type,
        sort=sort,
        order=order,
        page=page,
        page_size=page_size,
    )


@app.get("/api/facets")
async def api_facets():
    return get_facets()


@app.get("/api/sync-status")
async def api_sync_status():
    return get_sync_status()


@app.post("/api/sync")
async def api_sync():
    status = get_sync_status()
    if status["running"]:
        return {"started": False, "message": "sync already running"}
    asyncio.create_task(sync_pokemon_data_async())
    return {"started": True}
