"""Etapa 3: re-casa lo ya descargado contra el árbol por taxonID, sin volver a bajar nada.

Por cada fuente escribe v2/enriched/<fuente>.parquet con taxonID y método de
casado, y deja todo lo descartado en v2/rejects.parquet con su razón.

    python pipeline/03_enrich.py
"""
import sys

import duckdb

from config import DATA, ENRICHED, OUT, REJECTS, TAXA, WIKIDATA_DIR
from match import load_index, match_taxon

# route: wiki_url (título en source_url), wiki_name (título = nombre de la llave),
# wikidata (item), name (nombre con contexto de reino), id (la llave ya es de GBIF).
SOURCES = [
    ("wikipedia", "descriptions/wikipedia_descriptions.csv", "wiki_url", None),
    ("wikimedia", "images/wikimedia_images.csv", "wiki_name", None),
    ("wikidata_names", "wikidata/wikidata_common_names.csv", "wikidata", None),
    ("powo", "powo/powo_plants.csv", "name", "Plantae"),
    ("fishbase", "fishbase/fishbase_fish.csv", "name", "Animalia"),
    ("xeno_canto", "xeno_canto/xeno_canto_sounds.csv", "name", "Animalia"),
    ("gbif_descriptions", "backbone_csv/backbone_descriptions.csv", "id", None),
    ("gbif_images", "backbone_csv/backbone_images.csv", "id", None),
    ("gbif_references", "backbone_csv/backbone_references.csv", "id", None),
    ("gbif_types", "backbone_csv/backbone_types.csv", "id", None),
    ("gbif_vernacular_es", "backbone_csv/backbone_vernacular_es.csv", "id", None),
    ("conservation", "species_conservation.csv", "id", None),
    ("distribution", "species_country_rels.csv", "id", None),
]


def read_source(path):
    return (f"read_csv('{(DATA / path).as_posix()}', header=true, all_varchar=true, delim=',', "
            "quote='\"', escape='\"', max_line_size=30000000)")


def load_wikidata(con):
    # P846 admite "valor desconocido"; TRY_CAST lo deja en NULL.
    gbif = """TRY_CAST(trim(gbif, '"') AS BIGINT)"""
    item = "regexp_extract(item, 'entity/(Q[0-9]+)>', 1)"
    con.execute(f"""
        CREATE TEMP TABLE wd_gbif AS
        SELECT {item} AS item, {gbif} AS gbif
        FROM read_csv('{(WIKIDATA_DIR / "gbif_ids.tsv").as_posix()}', delim='\t', header=true,
                      quote='', escape='', names=['item', 'gbif'], all_varchar=true)
    """)
    con.execute("CREATE TEMP TABLE wd_title(lang VARCHAR, title VARCHAR, gbif BIGINT)")
    for lang in ("en", "es"):
        con.execute(f"""
            INSERT INTO wd_title
            SELECT '{lang}', url_decode(regexp_extract(title, '/wiki/(.*)>', 1)), {gbif}
            FROM read_csv('{(WIKIDATA_DIR / f"gbif_{lang}.tsv").as_posix()}', delim='\t',
                          header=true, quote='', escape='', names=['item', 'gbif', 'title'],
                          all_varchar=true)
        """)


def qkey_of(route):
    """Una consulta por qkey: lo que define a qué taxón pertenece la fila."""
    return {
        "wiki_url": "src.source_url",
        "wikidata": "src.species_key || '|' || src.wikidata_id",
    }.get(route, "src.species_key")


def build_queries(con, source, route, kingdom):
    keys = f"""SELECT DISTINCT {qkey_of(route)} AS qkey, TRY_CAST(src.species_key AS BIGINT) AS k
               {", src.lang, src.source_url" if route == "wiki_url" else ""}
               {", src.wikidata_id" if route == "wikidata" else ""}
               FROM {source} src"""
    context = f"'{kingdom}'" if kingdom else "NULL"
    gbif = {
        "wiki_url": ("w.gbif", """LEFT JOIN wd_title w ON w.lang = s.lang
                     AND w.title = url_decode(regexp_extract(s.source_url, '/wiki/(.*)$', 1))"""),
        "wiki_name": ("w.gbif", "LEFT JOIN wd_title w ON w.title = replace(b.name, ' ', '_')"),
        "wikidata": ("w.gbif", "LEFT JOIN wd_gbif w ON w.item = s.wikidata_id"),
        "name": ("NULL::BIGINT", ""),
        "id": ("s.k", ""),
    }[route]
    name = "NULL::VARCHAR" if route == "id" else "b.name"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE q AS
        SELECT s.qkey, {gbif[0]} AS gbif, {name} AS name, {context}::VARCHAR AS kingdom
        FROM ({keys}) s
        LEFT JOIN backbone b ON s.k = b.id
        {gbif[1]}
    """)


def main():
    ENRICHED.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{(OUT / '.tmp').as_posix()}'")
    load_index(con)
    load_wikidata(con)
    con.execute("""CREATE TEMP TABLE rejects(source VARCHAR, qkey VARCHAR, name VARCHAR,
                   reason VARCHAR)""")

    for name, path, route, kingdom in SOURCES:
        source = read_source(path)
        build_queries(con, source, route, kingdom)
        match_taxon(con, "q")
        con.execute(f"INSERT INTO rejects SELECT '{name}', qkey, name, reason FROM unmatched")
        con.execute(f"""
            COPY (SELECT m.taxonID, m.method, src.* EXCLUDE (species_key)
                  FROM {source} src JOIN matched m ON {qkey_of(route)} = m.qkey)
            TO '{(ENRICHED / f"{name}.parquet").as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """)
        methods = con.execute("SELECT method, count(*) FROM matched GROUP BY 1 ORDER BY 2 DESC").fetchall()
        reasons = con.execute("SELECT reason, count(*) FROM unmatched GROUP BY 1 ORDER BY 2 DESC").fetchall()
        print(f"{name}: casados {dict(methods)}; descartados {dict(reasons)}", flush=True)

    con.execute(f"COPY rejects TO '{REJECTS.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    strays = con.execute(f"""
        SELECT count(*) FROM read_parquet('{(ENRICHED / "*.parquet").as_posix()}') e
        ANTI JOIN read_parquet('{TAXA.as_posix()}') t ON e.taxonID = t.taxonID
    """).fetchone()[0]
    if strays:
        sys.exit(f"{strays} filas apuntan a un taxonID que no es nodo")
    print(f"Escrito en {ENRICHED}")


if __name__ == "__main__":
    main()
