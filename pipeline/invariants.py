"""Invariantes del árbol. Revisan el archivo ya escrito, no el SQL que lo generó."""
from config import RANK_LEVEL

ROOT_ID = -1
# Los dos Cepolidae que v1 fundió en un nodo por llavear con el nombre.
CEPOLIDAE = {4257: "Mollusca", 6755596: "Chordata"}


def check(con, taxa, synonyms=None, accepted_by_rank=None):
    """Regresa la lista de invariantes rotos; vacía significa que todo pasó."""
    con.execute(f"CREATE OR REPLACE TEMP VIEW taxa AS SELECT * FROM read_parquet('{taxa}')")
    levels = ", ".join(f"('{r}', {n})" for r, n in RANK_LEVEL.items())
    con.execute("CREATE OR REPLACE TEMP TABLE inv_levels(rank VARCHAR, level INT)")
    con.execute(f"INSERT INTO inv_levels VALUES {levels}")

    # Baja desde la raíz con tope de profundidad: un ciclo o un huérfano nunca se
    # alcanza, y un nodo con dos padres aparece dos veces. Siempre termina.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE reached AS
        WITH RECURSIVE r(id, kingdom_id, depth) AS (
            SELECT taxonID, NULL::BIGINT, 0 FROM taxa WHERE taxonID = {ROOT_ID}
            UNION ALL
            SELECT t.taxonID,
                   CASE WHEN t.rank = 'kingdom' THEN t.taxonID ELSE r.kingdom_id END,
                   r.depth + 1
            FROM taxa t JOIN r ON t.parentID = r.id
            WHERE r.depth < {len(RANK_LEVEL)}
        )
        SELECT * FROM r
    """)

    failures = []

    def expect_zero(label, sql):
        n = con.execute(sql).fetchone()[0]
        if n:
            failures.append(f"{label}: {n}")

    expect_zero("taxonID repetido", "SELECT count(*) - count(DISTINCT taxonID) FROM taxa")
    expect_zero("nodo sin padre que no es Biota",
                f"SELECT count(*) FROM taxa WHERE parentID IS NULL AND taxonID <> {ROOT_ID}")
    expect_zero("Biota con padre",
                f"SELECT count(*) FROM taxa WHERE parentID IS NOT NULL AND taxonID = {ROOT_ID}")
    expect_zero("padre inexistente", """
        SELECT count(*) FROM taxa c ANTI JOIN taxa p ON c.parentID = p.taxonID
        WHERE c.parentID IS NOT NULL""")
    expect_zero("rango desconocido", "SELECT count(*) FROM taxa ANTI JOIN inv_levels USING (rank)")
    expect_zero("padre sin rango superior", """
        SELECT count(*) FROM taxa c
        JOIN inv_levels lc ON c.rank = lc.rank
        JOIN taxa p ON c.parentID = p.taxonID
        JOIN inv_levels lp ON p.rank = lp.rank
        WHERE lp.level >= lc.level""")
    expect_zero("nodo inalcanzable desde Biota (ciclo o huérfano)",
                "SELECT count(*) FROM taxa ANTI JOIN reached ON taxonID = id")
    expect_zero("nodo alcanzado por más de un camino",
                "SELECT count(*) FROM (SELECT id FROM reached GROUP BY id HAVING count(*) > 1)")
    expect_zero("especie sin reino", """
        SELECT count(*) FROM taxa t JOIN reached r ON t.taxonID = r.id
        WHERE t.rank = 'species' AND r.kingdom_id IS NULL""")
    expect_zero("reino del árbol distinto al de GBIF", """
        SELECT count(*) FROM taxa t
        JOIN reached r ON t.taxonID = r.id
        JOIN taxa k ON r.kingdom_id = k.taxonID
        WHERE t.kingdom IS DISTINCT FROM k.name""")

    # Caso de regresión: mismo nombre, llaves y linajes distintos.
    ids = {row[0] for row in con.execute(
        "SELECT taxonID FROM taxa WHERE name = 'Cepolidae' AND rank = 'family'").fetchall()}
    if ids != set(CEPOLIDAE):
        failures.append(f"Cepolidae esperados {sorted(CEPOLIDAE)}, hay {sorted(ids)}")
    else:
        for family, phylum in CEPOLIDAE.items():
            lineage = con.execute(f"""
                WITH RECURSIVE up(id, parent, name, rank, depth) AS (
                    SELECT taxonID, parentID, name, rank, 0 FROM taxa WHERE taxonID = {family}
                    UNION ALL
                    SELECT t.taxonID, t.parentID, t.name, t.rank, up.depth + 1
                    FROM taxa t JOIN up ON t.taxonID = up.parent
                    WHERE up.depth < {len(RANK_LEVEL)}
                )
                SELECT name FROM up WHERE rank = 'phylum'""").fetchall()
            if lineage != [(phylum,)]:
                failures.append(f"Cepolidae {family}: esperaba {phylum}, hay {lineage}")

    if synonyms:
        expect_zero("sinónimo sin taxón aceptado", f"""
            SELECT count(*) FROM read_parquet('{synonyms}') s
            ANTI JOIN taxa t ON s.acceptedID = t.taxonID""")

    for rank, expected in (accepted_by_rank or {}).items():
        got = con.execute(
            f"SELECT count(*) FROM taxa WHERE rank = '{rank}' AND status = 'accepted'").fetchone()[0]
        if got != expected:
            failures.append(f"aceptados en {rank}: backbone {expected}, salida {got}")

    return failures
