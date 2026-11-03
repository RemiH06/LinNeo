"""Conexion a Neo4j para LinNeo."""

import os
from pathlib import Path
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Configuracion en el .env de la raiz del repo (plantilla: .env.example).
# Las variables de entorno ya definidas (p. ej. en Docker) tienen prioridad.
load_dotenv(Path(__file__).resolve().parents[2] / ".env")


def _load_secret(key: str, default: str = "") -> str:
    return os.getenv(key) or default


NEO4J_URI = _load_secret("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = _load_secret("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = _load_secret("NEO4J_PASSWORD", "")   # vacio -> sin autenticacion
NEO4J_DATABASE = _load_secret("NEO4J_DATABASE", "linneo")

_driver = None


def get_driver():
    global _driver
    if _driver is None:
        # Si no hay password (auth desactivada en neo4j.conf), conectar sin credenciales.
        if NEO4J_PASSWORD:
            _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        else:
            _driver = GraphDatabase.driver(NEO4J_URI, auth=None)
    return _driver


def run_query(cypher: str, params: dict = None):
    """Ejecuta una query y devuelve lista de dicts."""
    driver = get_driver()
    with driver.session(database=NEO4J_DATABASE) as session:
        result = session.run(cypher, params or {})
        return [record.data() for record in result]


def close_driver():
    global _driver
    if _driver is not None:
        _driver.close()
        _driver = None