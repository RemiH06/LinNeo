"""Casado de registros externos contra el árbol. Ambiguo = descartar y registrar.

Trabaja por lotes en SQL. La entrada es una tabla con columnas
(qkey, gbif, name, kingdom); un qkey puede repetirse si trae varios IDs.

Orden de confianza:
  1. id:           el ID de GBIF resuelve a un solo nodo (directo o por sinónimo).
  2. name:         el nombre coincide con un solo nodo.
  3. synonym_name: no hay nodo con ese nombre y coincide con un solo sinónimo.

Si el ID existe en el backbone pero no es nodo, su reino acota la búsqueda por
nombre: así un `doubtful` no cae en un homónimo de otro reino.
"""
import duckdb

from config import SYNONYMS, TAXA, TAXON_TSV


def unique_names(kingdoms=None, rank=None):
    """Nombres que señalan a un solo nodo, para consultar fuentes que solo reciben nombre.

    Un homónimo no se pregunta: la respuesta no diría a cuál de los dos se refiere.
    """
    where = ["taxonID >= 0"]
    if kingdoms:
        where.append("kingdom IN (" + ", ".join(f"'{k}'" for k in kingdoms) + ")")
    if rank:
        where.append(f"rank = '{rank}'")
    return duckdb.sql(f"""
        SELECT taxonID AS species_key, name AS canonical_name
        FROM read_parquet('{TAXA.as_posix()}')
        WHERE {" AND ".join(where)}
        QUALIFY count(*) OVER (PARTITION BY lower(name)) = 1
    """).df()


def name_map(kingdoms=None, rank=None):
    """{nombre en minúsculas: taxonID} sobre unique_names."""
    df = unique_names(kingdoms, rank)
    return dict(zip(df["canonical_name"].str.lower(), df["species_key"].astype(int)))


def load_index(con):
    """Crea backbone, node_of y name_index."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE backbone AS
        SELECT taxonID::BIGINT AS id, coalesce(canonicalName, scientificName) AS name, kingdom
        FROM read_csv('{TAXON_TSV.as_posix()}', delim='\t', header=true, quote='', escape='',
                      all_varchar=true, null_padding=true, strict_mode=false)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE node_of AS
        SELECT taxonID AS id, taxonID FROM read_parquet('{TAXA.as_posix()}')
        UNION ALL
        SELECT taxonID AS id, acceptedID FROM read_parquet('{SYNONYMS.as_posix()}')
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE name_index AS
        SELECT lower(name) AS name, taxonID, 1 AS tier, kingdom
        FROM read_parquet('{TAXA.as_posix()}')
        UNION ALL
        SELECT lower(s.name), t.taxonID, 2, t.kingdom
        FROM read_parquet('{SYNONYMS.as_posix()}') s
        JOIN read_parquet('{TAXA.as_posix()}') t ON s.acceptedID = t.taxonID
    """)


def match_taxon(con, queries):
    """Llena `matched(qkey, taxonID, method)` y `unmatched(qkey, name, reason)`."""
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE by_id AS
        SELECT q.qkey, min(n.taxonID) AS taxonID, count(DISTINCT n.taxonID) AS hits
        FROM {queries} q JOIN node_of n ON q.gbif = n.id
        GROUP BY q.qkey
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE named AS
        SELECT q.qkey, any_value(q.name) AS name,
               coalesce(any_value(q.kingdom), any_value(b.kingdom)) AS kingdom
        FROM {queries} q
        LEFT JOIN backbone b ON q.gbif = b.id
        ANTI JOIN by_id i ON q.qkey = i.qkey
        GROUP BY q.qkey
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE by_name AS
        SELECT q.qkey, x.tier, min(x.taxonID) AS taxonID, count(DISTINCT x.taxonID) AS hits
        FROM named q
        JOIN name_index x ON lower(q.name) = x.name
                         AND (q.kingdom IS NULL OR q.kingdom = x.kingdom)
        GROUP BY q.qkey, x.tier
        QUALIFY row_number() OVER (PARTITION BY q.qkey ORDER BY x.tier) = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE matched AS
        SELECT qkey, taxonID, 'id' AS method FROM by_id WHERE hits = 1
        UNION ALL
        SELECT qkey, taxonID, CASE tier WHEN 1 THEN 'name' ELSE 'synonym_name' END
        FROM by_name WHERE hits = 1
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE unmatched AS
        SELECT qkey, NULL::VARCHAR AS name, 'ambiguous_id' AS reason FROM by_id WHERE hits > 1
        UNION ALL
        SELECT n.qkey, n.name,
               CASE WHEN b.hits IS NULL THEN 'not_found' ELSE 'ambiguous_name' END
        FROM named n LEFT JOIN by_name b ON n.qkey = b.qkey
        WHERE b.hits IS NULL OR b.hits > 1
    """)
