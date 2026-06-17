"""
LinNeo API -- FastAPI sobre el grafo Neo4j.

Ejecutar:
    cd backend
    pip install -r requirements.txt
    uvicorn app.main:app --reload

Docs interactivas: http://localhost:8000/docs
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from . import queries
from .db import close_driver

app = FastAPI(
    title="LinNeo API",
    description="Consulta de biodiversidad sobre el grafo LinNeo",
    version="1.0.0",
)

# CORS: permitir que el frontend (otro origen) consuma la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # en produccion, restringir al dominio del front
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("shutdown")
def shutdown():
    close_driver()


@app.get("/")
def root():
    return {"name": "LinNeo API", "status": "ok", "docs": "/docs"}


@app.get("/stats")
def get_stats():
    """Estadisticas generales del grafo."""
    return queries.stats()


@app.get("/search")
def search(
    q: str = Query(..., min_length=2, description="Termino de busqueda"),
    limit: int = Query(25, ge=1, le=100),
):
    """Busca especies por nombre cientifico o comun."""
    return {"query": q, "results": queries.search_by_name(q, limit)}


@app.get("/search/description")
def search_description(
    q: str = Query(..., min_length=3, description="Texto a buscar en descripciones"),
    limit: int = Query(25, ge=1, le=100),
):
    """Busca un termino dentro de las descripciones de las especies."""
    return {"query": q, "results": queries.search_in_descriptions(q, limit)}


@app.get("/species/{species_key}")
def species_detail(species_key: int):
    """Ficha completa de una especie."""
    detail = queries.get_species_detail(species_key)
    if detail is None:
        raise HTTPException(status_code=404, detail="Especie no encontrada")
    detail["lineage"] = queries.get_taxonomy_path(species_key)
    detail["relatives"] = queries.get_relatives(species_key)
    return detail


@app.get("/filter")
def filter_species(
    kingdom: Optional[str] = None,
    country: Optional[str] = None,
    habit: Optional[str] = None,
    limit: int = Query(50, ge=1, le=200),
):
    """Filtra especies por reino, pais y/o habito."""
    return {
        "filters": {"kingdom": kingdom, "country": country, "habit": habit},
        "results": queries.filter_species(kingdom, country, habit, limit),
    }


@app.get("/taxon/{rank}/{key}")
def taxon_node(rank: str, key: int):
    """Vista de un nodo taxonomico no-especie: hijos directos + paises agregados."""
    node = queries.get_taxon_node(rank, key)
    if node is None:
        raise HTTPException(status_code=404, detail="Nodo taxonomico no encontrado")
    return node


# ── SHUI: grafo principal, ejemplos por reino, geografia ──

@app.get("/kingdoms")
def kingdoms():
    return queries.list_kingdoms()


@app.get("/graph")
def graph():
    """Grafo inicial: Biota -> reinos -> filos."""
    return queries.graph_default()


@app.get("/graph/{rank}/{key}")
def graph_focus(rank: str, key: int):
    """Grafo centrado en un nodo + 2 niveles de descendientes."""
    g = queries.graph_focus(rank, key)
    if g is None:
        raise HTTPException(status_code=404, detail="Nodo no encontrado")
    return g


@app.get("/random/kingdoms")
def random_kingdoms():
    """Una especie aleatoria con descripcion por cada reino."""
    return queries.random_by_kingdom(1)


@app.get("/random/{rank}/{key}")
def random_descendants(rank: str, key: int, n: int = Query(9, ge=1, le=20)):
    """Especies aleatorias con descripcion descendientes de un nodo."""
    return queries.random_descendants(rank, key, n)


@app.get("/continents")
def continents():
    return queries.list_continents()


@app.get("/continents/{continent}/countries")
def continent_countries(continent: str):
    return queries.countries_in_continent(continent)

#? No sé qué le pasó al main pero ya no muestra las cosas como deberían estar.
#! El color en los nodos del grafo taxonómico desapareció, corrígelo
#* Fue un día productivo, por favor vete a dormir.
# TODO dormir