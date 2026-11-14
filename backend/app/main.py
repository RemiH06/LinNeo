"""LinNeo API: FastAPI de solo lectura sobre el grafo de Neo4j.

    cd backend
    uvicorn app.main:app --reload --port 8035

Docs interactivas en http://localhost:8035/docs (cerradas con LINNEO_ENV=prod).
"""

import os
from contextlib import asynccontextmanager
from typing import List, Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

from . import db, queries

API_VERSION = "2.1.0"
PROD = os.getenv("LINNEO_ENV") == "prod"
# En prod el front y la API comparten origen; CORS solo hace falta en desarrollo.
CORS_ORIGINS = [o for o in os.getenv("CORS_ORIGINS", "http://localhost:5135").split(",") if o]
CACHE_MAX_AGE = 3600

Rank = Literal["domain", "kingdom", "phylum", "class", "order", "family", "genus", "species"]
Mode = Literal["contains", "starts"]


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


# Después del de caché para quedar por fuera: así también los 304 llevan CORS.
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
    q: str = Query(..., min_length=2, max_length=100),
    limit_per_group: int = Query(100, ge=1, le=200),
    mode: Mode = "contains",
):
    """Subcadena del nombre en todos los rangos, agrupada por rango y reino."""
    return {"query": q, "groups": queries.search_clades(q, limit_per_group, mode)}


@app.get("/search/clades/{rank}")
def search_clades_by_rank(
    rank: Rank,
    q: str = Query(..., min_length=2, max_length=100),
    limit_per_group: int = Query(100, ge=1, le=200),
    mode: Mode = "contains",
):
    """Lo mismo en un solo rango, para que el front pinte cada rango en cuanto llega."""
    return {"query": q, "rank": rank,
            "groups": queries.search_clades_by_rank(q, rank, limit_per_group, mode)}


@app.get("/filter")
def filter_species(
    country: str = Query(..., min_length=2, max_length=2, description="ISO 3166-1 alfa-2"),
    kingdom: str | None = None,
    habit: str | None = Query(None, max_length=50),
    offset: int = Query(0, ge=0),
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
def species(key: int):
    """Ficha completa de una especie, con linaje y parientes."""
    return _found(queries.species_detail(key), "Especie")


@app.get("/taxon/{key}")
def taxon(key: int, offset: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=1000)):
    """Taxón superior: hijos directos paginados, linaje y países de sus especies."""
    return _found(queries.taxon_node(key, offset, limit), "Taxón")


@app.get("/taxon/{key}/gallery")
def taxon_gallery(key: int, offset: int = Query(0, ge=0), limit: int = Query(250, ge=1, le=500)):
    """Especies con imagen bajo el taxón, a cualquier profundidad."""
    return _found(queries.taxon_gallery(key, offset, limit), "Taxón")


@app.get("/taxon/{key}/sounds")
def taxon_sounds(key: int, limit: int = Query(250, ge=1, le=500)):
    """Árbol del taxón hasta sus especies con sonido."""
    return _found(queries.taxon_sound_tree(key, limit), "Taxón")


@app.get("/taxon/{key}/infographic")
def taxon_infographic(key: int):
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
def graph_focus(key: int):
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
def random_descendants(key: int, n: int = Query(9, ge=1, le=20)):
    """Especies con descripción al azar bajo un taxón, de ramas distintas."""
    return queries.random_descendants(key, n)


# ── Geografía ──

@app.get("/map/country/{code}")
def map_country(
    code: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=1000),
):
    """Especies presentes en un país, con el mismo formato que /taxon."""
    return _found(queries.country_node(code, offset, limit), "País")
