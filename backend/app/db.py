"""Conexión a Neo4j: un driver por proceso, abierto y cerrado en el lifespan de la API."""

import os
from pathlib import Path

from dotenv import load_dotenv
from neo4j import READ_ACCESS, GraphDatabase, Query

# Configuración en el .env de la raíz del repo (plantilla: .env.example).
# Las variables ya definidas (Docker, tests) tienen prioridad.
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "")  # vacío = sin autenticación, solo en local
DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")  # Community solo tiene `neo4j`
# Tope por consulta: una ruta pública no debe poder ocupar Neo4j indefinidamente.
TIMEOUT = float(os.getenv("NEO4J_QUERY_TIMEOUT", "15"))

_driver = None


def connect():
    global _driver
    _driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD) if PASSWORD else None)
    _driver.verify_connectivity()


def close():
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None


def run_query(cypher, params=None):
    """Ejecuta una consulta de lectura y devuelve una lista de dicts."""
    with _driver.session(database=DATABASE, default_access_mode=READ_ACCESS) as session:
        result = session.run(Query(cypher, timeout=TIMEOUT), params or {})
        return [record.data() for record in result]
