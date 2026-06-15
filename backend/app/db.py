"""Conexion a Neo4j para LinNeo."""

import os
from pathlib import Path
from neo4j import GraphDatabase

# Cargar credenciales de .secrets o .env (mismo patron que los fetchers)
def _load_secret(key: str, default: str = "") -> str:
    val = os.getenv(key)
    if val:
        return val
    for fname in [".secrets", ".env", "../.secrets", "../../.secrets"]:
        p = Path(fname)
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith(key):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return default


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