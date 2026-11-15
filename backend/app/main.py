"""LinNeo API: FastAPI de solo lectura sobre el grafo de Neo4j.

    cd backend
    uvicorn app.main:app --reload --port 8035

Docs interactivas en http://localhost:8035/docs, solo con LINNEO_ENV=dev: sin la
variable la API se comporta como producción.
"""

import asyncio
import ipaddress
import math
import os
import re
import time
from contextlib import asynccontextmanager
from typing import Annotated, List, Literal

from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from neo4j.exceptions import DriverError, Neo4jError

from . import db, queries

API_VERSION = "2.7.0"
PROD = os.getenv("LINNEO_ENV") != "dev"
# En prod el front y la API comparten origen; CORS solo hace falta en desarrollo.
CORS_ORIGINS = [o for o in os.getenv("CORS_ORIGINS", "http://localhost:5135").split(",") if o]
CACHE_MAX_AGE = 3600
# Límite por cliente (cubeta de fichas): ráfaga de RATE_BURST fichas que se recarga
# a RATE_PER_SEC por segundo. La ráfaga cubre recorrer el sitio entero de corrido
# (recorrido.py, ver steps.md); un raspador sostenido sí choca.
RATE_BURST = int(os.getenv("LINNEO_RATE_BURST", "120"))
RATE_PER_SEC = float(os.getenv("LINNEO_RATE_PER_SEC", "2"))
_buckets: dict[str, tuple[float, float]] = {}  # cliente -> (fichas, último momento)
_last_purge = 0.0
# Rutas que pueden tardar segundos en frío (subcadena, galería, país; medido en
# Pixilate: 2 a 7 s). Gastan HEAVY_COST fichas y comparten HEAVY_SLOTS lugares, para
# que no acaparen los hilos de la API. Sin lugar se espera (en el event loop, sin
# ocupar hilo) hasta HEAVY_WAIT s: la página de resultados pide 8 rangos a la vez.
HEAVY = re.compile(r"^/(search/clades|filter|map/country/|taxon/-?\d+/(gallery|sounds|infographic))")
HEAVY_COST = 4
HEAVY_SLOTS = int(os.getenv("LINNEO_HEAVY_SLOTS", "4"))
HEAVY_WAIT = 10
_heavy = asyncio.Semaphore(HEAVY_SLOTS)
MAX_OFFSET = 100_000

Rank = Literal["domain", "kingdom", "phylum", "class", "order", "family", "genus", "species"]
Mode = Literal["contains", "starts"]
# taxonID cabe en un entero de 64 bits; uno más grande haría fallar al driver (500).
Key = Annotated[int, Path(ge=-(2**63), lt=2**63)]
Offset = Annotated[int, Query(ge=0, le=MAX_OFFSET)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.connect()
    # La data solo cambia con un import y el formato con una versión de la API.
    app.state.etag = f'"{queries.data_version()}-{API_VERSION}"'
    yield
    db.close()


app = FastAPI(
    title="LinNeo API",
    description="Consulta de biodiversidad sobre el grafo LinNeo",
    version=API_VERSION,
    lifespan=lifespan,
    docs_url=None if PROD else "/docs",
    redoc_url=None,
    openapi_url=None if PROD else "/openapi.json",
)


def _is_random(path):
    return path.startswith("/random") or path.endswith("/infographic")


@app.middleware("http")
async def http_cache(request: Request, call_next):
    """ETag por versión de datos: un If-None-Match vigente responde 304 sin tocar Neo4j.

    Las rutas al azar no se guardan en caché.
    """
    if request.method != "GET":
        return await call_next(request)
    if _is_random(request.url.path):
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response
    etag = request.app.state.etag
    headers = {"ETag": etag, "Cache-Control": f"public, max-age={CACHE_MAX_AGE}"}
    sent = request.headers.get("if-none-match", "")
    if etag in (tag.strip().removeprefix("W/") for tag in sent.split(",")):
        return Response(status_code=304, headers=headers)
    response = await call_next(request)
    if response.status_code == 200:
        response.headers.update(headers)
    return response


def _client_key(request):
    # En prod la única entrada es el túnel de Cloudflare, que pone CF-Connecting-IP
    # (y lo sobrescribe si el cliente lo manda). Fuera de prod se ignora: sin
    # Cloudflare delante, cualquiera lo falsificaría para saltarse el límite.
    ip = request.headers.get("cf-connecting-ip") if PROD else None
    ip = ip or (request.client.host if request.client else "")
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return ip
    # Un hogar o un VPS con IPv6 recibe un /64 entero: rotar dentro de él no da otra cubeta.
    return str(ipaddress.ip_network(f"{addr}/64", strict=False)) if addr.version == 6 else ip


def _take_token(key, now, cost=1):
    """Gasta `cost` fichas del cliente; devuelve 0 si pasa o los segundos que debe esperar."""
    global _last_purge
    full = RATE_BURST / RATE_PER_SEC
    if now - _last_purge > full:  # una vez por ciclo: olvida a quien ya recargó la cubeta
        for k in [k for k, (_, last) in _buckets.items() if now - last > full]:
            del _buckets[k]
        _last_purge = now
    tokens, last = _buckets.get(key, (RATE_BURST, now))
    tokens = min(RATE_BURST, tokens + (now - last) * RATE_PER_SEC)
    if tokens < cost:
        _buckets[key] = (tokens, now)
        return (cost - tokens) / RATE_PER_SEC
    _buckets[key] = (tokens - cost, now)
    return 0


def _busy(detail, status, retry_after):
    return JSONResponse({"detail": detail}, status_code=status,
                        headers={"Retry-After": str(math.ceil(retry_after))})


@app.middleware("http")
async def rate_limit(request: Request, call_next):
    """Por fuera del de caché: un 304 también gasta ficha. Corre en el event loop, sin hilos."""
    heavy = bool(HEAVY.match(request.url.path))
    wait = _take_token(_client_key(request), time.monotonic(), HEAVY_COST if heavy else 1)
    if wait:
        return _busy("Demasiadas peticiones; intenta en unos segundos", 429, wait)
    if not heavy:
        return await call_next(request)
    try:
        await asyncio.wait_for(_heavy.acquire(), HEAVY_WAIT)
    except TimeoutError:
        return _busy("El servidor está ocupado; intenta en unos segundos", 503, 5)
    try:
        return await call_next(request)
    finally:
        _heavy.release()


@app.exception_handler(Neo4jError)
@app.exception_handler(DriverError)
async def neo4j_unavailable(request: Request, exc):
    """Tope de tiempo de la consulta o Neo4j caído: 503 (reintentable), no 500."""
    if isinstance(exc, DriverError) or "TimedOut" in (exc.code or ""):
        return _busy("La consulta tardó demasiado o la base no responde", 503, 5)
    return JSONResponse({"detail": "Error interno"}, status_code=500)


# Después de los otros para quedar por fuera: así los 304 y los 429 también llevan CORS.
app.add_middleware(CORSMiddleware, allow_origins=CORS_ORIGINS, allow_methods=["GET"])


def _found(value, what):
    if value is None:
        raise HTTPException(status_code=404, detail=f"{what} no encontrado")
    return value


@app.get("/")
def root():
    return {"name": "LinNeo API", "version": API_VERSION, "status": "ok"}


@app.get("/stats")
def stats():
    """Totales del grafo."""
    return queries.stats()


# ── Búsqueda ──

@app.get("/search")
def search(
    q: str = Query(..., min_length=2, max_length=100, description="Nombre científico, sinónimo o común"),
    offset: int = Query(0, ge=0, lt=queries.SEARCH_WINDOW),
    limit: int = Query(25, ge=1, le=100),
):
    """Búsqueda de taxones de cualquier rango: exactos primero, luego los más conocidos."""
    found = queries.search(q, offset, limit)
    return {"query": q, "offset": offset, "limit": limit, **found}


@app.get("/search/clades")
def search_clades(
    q: str = Query(..., min_length=3, max_length=100),
    limit_per_group: int = Query(100, ge=1, le=200),
    mode: Mode = "contains",
):
    """Subcadena del nombre en todos los rangos, agrupada por rango y reino."""
    return {"query": q, "groups": queries.search_clades(q, limit_per_group, mode)}


@app.get("/search/clades/{rank}")
def search_clades_by_rank(
    rank: Rank,
    # Con 2 letras la subcadena ordena cientos de miles de nombres: 3.3 s por rango
    # (medido en Pixilate); con 3 baja a 0.5 s.
    q: str = Query(..., min_length=3, max_length=100),
    limit_per_group: int = Query(100, ge=1, le=200),
    mode: Mode = "contains",
):
    """Lo mismo en un solo rango, para que el front pinte cada rango en cuanto llega."""
    return {"query": q, "rank": rank,
            "groups": queries.search_clades_by_rank(q, rank, limit_per_group, mode)}


@app.get("/filter")
def filter_species(
    country: str = Query(..., min_length=2, max_length=2, description="ISO 3166-1 alfa-2"),
    kingdom: str | None = Query(None, max_length=40),
    habit: str | None = Query(None, max_length=50),
    offset: Offset = 0,
    limit: int = Query(50, ge=1, le=200),
):
    """Especies de un país, por reino y/o hábito, en orden de taxonID."""
    return {
        "filters": {"country": country, "kingdom": kingdom, "habit": habit},
        "offset": offset, "limit": limit,
        "results": queries.filter_species(country, kingdom, habit, offset, limit),
    }


# ── Especie y taxón ──

@app.get("/species/{key}")
def species(key: Key):
    """Ficha completa de una especie, con linaje y parientes."""
    return _found(queries.species_detail(key), "Especie")


@app.get("/taxon/{key}")
def taxon(key: Key, offset: Offset = 0, limit: int = Query(500, ge=1, le=1000)):
    """Taxón superior: hijos directos paginados, linaje y países de sus especies."""
    return _found(queries.taxon_node(key, offset, limit), "Taxón")


@app.get("/taxon/{key}/gallery")
def taxon_gallery(key: Key, offset: Offset = 0, limit: int = Query(250, ge=1, le=500)):
    """Especies con imagen bajo el taxón, a cualquier profundidad."""
    return _found(queries.taxon_gallery(key, offset, limit), "Taxón")


@app.get("/taxon/{key}/sounds")
def taxon_sounds(key: Key, limit: int = Query(250, ge=1, le=500)):
    """Árbol del taxón hasta sus especies con sonido."""
    return _found(queries.taxon_sound_tree(key, limit), "Taxón")


@app.get("/taxon/{key}/infographic")
def taxon_infographic(key: Key):
    """Datos del póster de un taxón; las especies destacadas cambian en cada llamada."""
    return _found(queries.taxon_infographic(key), "Taxón")


# ── Grafo de exploración y ejemplos ──

@app.get("/kingdoms")
def kingdoms():
    return queries.kingdoms()


@app.get("/graph")
def graph():
    """Grafo inicial: Biota, dominios y reinos."""
    return queries.graph_focus(-1)


@app.get("/graph/{key}")
def graph_focus(key: Key):
    """Grafo centrado en un taxón, con dos niveles de descendientes."""
    return _found(queries.graph_focus(key), "Taxón")


@app.get("/random/pool")
def random_pool(
    kingdoms: List[str] = Query(default=[], description="Reinos activos; vacío = todos"),
    n: int = Query(8, ge=1, le=30),
):
    """n especies con descripción, cada una de un reino elegido al azar entre `kingdoms`."""
    return queries.random_from_kingdoms(kingdoms, n)


@app.get("/random/{key}")
def random_descendants(key: Key, n: int = Query(9, ge=1, le=20)):
    """Especies con descripción al azar bajo un taxón, de ramas distintas."""
    return queries.random_descendants(key, n)


# ── Geografía ──

@app.get("/map/country/{code}")
def map_country(
    code: Annotated[str, Path(pattern=r"^[A-Za-z]{2}$")],
    offset: Offset = 0,
    limit: int = Query(500, ge=1, le=1000),
    kingdom: str | None = Query(None, max_length=40),
):
    """Especies presentes en un país, con el mismo formato que /taxon; kingdom filtra."""
    return _found(queries.country_node(code, offset, limit, kingdom), "País")
