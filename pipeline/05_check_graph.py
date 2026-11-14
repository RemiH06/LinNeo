"""Etapa 5: valida el grafo ya cargado en Neo4j contra el árbol de v2.

Repite en Cypher los invariantes que 01_backbone revisó sobre el parquet:
si el import perdió o duplicó algo, aquí se nota.

    python pipeline/05_check_graph.py
"""
import sys

import duckdb
from neo4j import GraphDatabase

from config import IMPORT_DIR, TAXA

URI = "bolt://localhost:7687"


def main():
    expected = dict(duckdb.sql(
        f"SELECT rank, count(*) FROM read_parquet('{TAXA.as_posix()}') GROUP BY rank").fetchall())
    found_in = duckdb.sql(
        f"SELECT count(*) FROM read_csv('{(IMPORT_DIR / 'rel_found_in.csv.gz').as_posix()}')"
    ).fetchall()[0][0]
    total = sum(expected.values())

    checks = {
        "nodos con más de un padre": ("MATCH (t:Taxon) WHERE count{(t)<-[:HAS_CHILD]-()} > 1 "
                                      "RETURN count(t)", 0),
        "nodos sin padre que no son Biota": ("MATCH (t:Taxon) WHERE NOT t:Biota "
                                             "AND NOT ()-[:HAS_CHILD]->(t) RETURN count(t)", 0),
        "HAS_CHILD": ("MATCH ()-[r:HAS_CHILD]->() RETURN count(r)", total - 1),
        "alcanzables desde Biota": ("MATCH (:Biota)-[:HAS_CHILD*]->(t) RETURN count(t)", total - 1),
        "FOUND_IN": ("MATCH ()-[r:FOUND_IN]->() RETURN count(r)", found_in),
        "Cepolidae en Mollusca y en Chordata": (
            "MATCH (p:Phylum)-[:HAS_CHILD*]->(f:Family {name: 'Cepolidae'}) "
            "RETURN count(DISTINCT p.name) + count(DISTINCT f)", 4),
    }
    # Los conteos precalculados de Biota cubren todas las especies (aggregates.py).
    checks["speciesCount de Biota"] = ("MATCH (b:Biota) RETURN b.speciesCount",
                                       expected["species"])
    for rank, n in expected.items():
        label = rank[0].upper() + rank[1:]
        checks[f"nodos {label}"] = (f"MATCH (t:{label}) RETURN count(t)", n)

    failures = []
    with GraphDatabase.driver(URI, auth=None) as driver:
        for label, (query, want) in checks.items():
            got = driver.execute_query(query).records[0][0]
            status = "OK" if got == want else "FALLA"
            print(f"{status:5} {label}: {got:,} (esperado {want:,})", flush=True)
            if got != want:
                failures.append(label)
        for label in ("Description", "Media", "Country", "Continent"):
            n = driver.execute_query(f"MATCH (n:{label}) RETURN count(n)").records[0][0]
            print(f"      {label}: {n:,}")
    if failures:
        sys.exit(f"Fallaron: {', '.join(failures)}")
    print("Grafo OK")


if __name__ == "__main__":
    main()
