"""Etapa 1: del GBIF Backbone al árbol llaveado por taxonID.

Entra Taxon.tsv y salen taxa.parquet (los nodos, con su único padre) y
synonyms.parquet (nombres para búsqueda y para casar fuentes externas).
Si se rompe un invariante no se publica ningún archivo.

    python pipeline/01_backbone.py
"""
import sys

import duckdb

from config import OUT, RANK_LEVEL, SYNONYMS, TAXA, TAXON_TSV
from invariants import check

MAX_HOPS = 20
GBIF_RANKS = [r for r in RANK_LEVEL if r not in ("biota", "domain")]
INFRASPECIFIC = ("subspecies", "variety", "form")

# GBIF no publica estos niveles. Llevan taxonID negativo: los de GBIF son >= 0.
SYNTHETIC = [
    (-1, None, "biota", "Biota"),
    (-2, -1, "domain", "Eukaryota"),
    (-3, -1, "domain", "Prokaryota"),
    (-4, -1, "domain", "Viruses"),
    (-5, -1, "domain", "incertae sedis"),
]
KINGDOM_DOMAIN = {
    "Animalia": -2, "Plantae": -2, "Fungi": -2, "Chromista": -2, "Protozoa": -2,
    "Bacteria": -3, "Archaea": -3,
    "Viruses": -4,
    "incertae sedis": -5,
}


def sql_list(values):
    return ", ".join(f"'{v}'" for v in values)


def load_backbone(con):
    # quote='' porque los nombres traen comillas sueltas; el TSV no entrecomilla.
    con.execute(f"""
        CREATE TEMP TABLE raw AS
        SELECT taxonID::BIGINT AS id,
               parentNameUsageID::BIGINT AS parent,
               acceptedNameUsageID::BIGINT AS accepted,
               coalesce(canonicalName, scientificName) AS name,
               scientificName,
               scientificNameAuthorship AS authorship,
               taxonRank AS rank,
               taxonomicStatus AS status,
               kingdom
        FROM read_csv('{TAXON_TSV.as_posix()}', delim='\t', header=true, quote='', escape='',
                      all_varchar=true, null_padding=true, strict_mode=false)
    """)
    levels = ", ".join(f"('{r}', {n})" for r, n in RANK_LEVEL.items())
    con.execute("CREATE TEMP TABLE levels(rank VARCHAR, level INT)")
    con.execute(f"INSERT INTO levels VALUES {levels}")


def resolve_parents(con):
    """Sube por parent hasta un ancestro modelado de rango estrictamente superior.

    Los `doubtful` cuentan como ancestros: colgar a sus hijos del abuelo perdería
    la familia real (Cepola es accepted, pero su Cepolidae es doubtful).
    También sube a las especies colgadas de otra especie.
    """
    con.execute(f"""
        CREATE TEMP TABLE cand AS
        SELECT r.*, l.level, r.parent AS anc
        FROM raw r JOIN levels l USING (rank)
        WHERE r.rank IN ({sql_list(GBIF_RANKS)}) AND r.status IN ('accepted', 'doubtful')
    """)
    for _ in range(MAX_HOPS):
        pending = con.execute("""
            UPDATE cand SET anc = a.parent
            FROM raw a LEFT JOIN levels l ON a.rank = l.rank
            WHERE cand.anc = a.id
              AND (l.level IS NULL OR l.level >= cand.level
                   OR a.status NOT IN ('accepted', 'doubtful'))
        """).fetchone()[0]
        if pending == 0:
            return
    sys.exit(f"Parentesco sin resolver tras {MAX_HOPS} saltos: revisa si hay ciclos")


def build_taxa(con):
    """Nodos = los accepted más los doubtful que son ancestros de alguno."""
    con.execute("""
        CREATE TEMP TABLE kept AS
        WITH RECURSIVE keep(id, anc) AS (
            SELECT id, anc FROM cand WHERE status = 'accepted'
            UNION
            SELECT c.id, c.anc FROM cand c JOIN keep k ON c.id = k.anc
        )
        SELECT id FROM keep
    """)
    mapping = ", ".join(f"('{name}', {dom})" for name, dom in KINGDOM_DOMAIN.items())
    con.execute(f"""
        CREATE TEMP TABLE taxa_out AS
        SELECT c.id AS taxonID,
               CASE WHEN c.rank = 'kingdom' THEN d.domain ELSE c.anc END AS parentID,
               c.rank, c.name, c.scientificName, c.authorship, c.status,
               CASE WHEN c.rank = 'kingdom' THEN c.name ELSE c.kingdom END AS kingdom
        FROM cand c
        SEMI JOIN kept k ON c.id = k.id
        LEFT JOIN (VALUES {mapping}) d(kingdom_name, domain)
               ON c.rank = 'kingdom' AND c.name = d.kingdom_name
    """)
    for taxon, parent, rank, name in SYNTHETIC:
        con.execute("INSERT INTO taxa_out VALUES (?, ?, ?, ?, ?, NULL, 'synthetic', NULL)",
                    [taxon, parent, rank, name, name])


def build_synonyms(con):
    """Un sinónimo apunta a un nodo, o a la especie de su infraespecífico."""
    con.execute(f"""
        CREATE TEMP TABLE syn_out AS
        SELECT s.id AS taxonID,
               coalesce(direct.taxonID, viaparent.taxonID) AS acceptedID,
               s.name, s.authorship, s.rank, s.status
        FROM raw s
        JOIN raw a ON s.accepted = a.id
        LEFT JOIN taxa_out direct ON a.id = direct.taxonID
        LEFT JOIN taxa_out viaparent
               ON a.parent = viaparent.taxonID AND a.rank IN ({sql_list(INFRASPECIFIC)})
        WHERE coalesce(direct.taxonID, viaparent.taxonID) IS NOT NULL
    """)


def report(con):
    print("Nodos por rango y estado:")
    for rank, status, n in con.execute("""
            SELECT t.rank, t.status, count(*)
            FROM taxa_out t JOIN levels l ON t.rank = l.rank
            GROUP BY t.rank, t.status, l.level ORDER BY l.level, t.status""").fetchall():
        print(f"  {rank:<8} {status:<10} {n:>9,}")
    total = con.execute("SELECT count(*) FROM raw WHERE accepted IS NOT NULL").fetchone()[0]
    kept = con.execute("SELECT count(*) FROM syn_out").fetchone()[0]
    print(f"Sinónimos: {kept:,} de {total:,} (el resto apunta a taxones que no son nodo)")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    taxa_tmp, syn_tmp = TAXA.with_suffix(".tmp"), SYNONYMS.with_suffix(".tmp")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order = false")

    load_backbone(con)
    resolve_parents(con)
    build_taxa(con)
    build_synonyms(con)
    report(con)

    accepted = dict(con.execute(f"""
        SELECT rank, count(*) FROM raw
        WHERE status = 'accepted' AND rank IN ({sql_list(GBIF_RANKS)})
        GROUP BY rank""").fetchall())
    for table, path in (("taxa_out", taxa_tmp), ("syn_out", syn_tmp)):
        con.execute(f"COPY {table} TO '{path.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")

    failures = check(con, taxa_tmp.as_posix(), syn_tmp.as_posix(), accepted)
    if failures:
        taxa_tmp.unlink()
        syn_tmp.unlink()
        sys.exit("Invariantes rotos:\n  " + "\n  ".join(failures))

    taxa_tmp.replace(TAXA)
    syn_tmp.replace(SYNONYMS)
    print(f"Invariantes OK. Escrito en {OUT}")


if __name__ == "__main__":
    main()
