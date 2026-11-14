"""Etapa 4: exporta v2 a CSV para `neo4j-admin database import full`.

Un archivo por tipo de nodo y de relación, y uno por fuente de textos y medios:
así se puede medir el store con y sin cada fuente eligiendo qué archivos importar.

Cuando la fila de un sinónimo y la del propio taxón caen en el mismo nodo,
gana la del propio taxón (source_key = taxonID).

Los taxones llevan conteos precalculados de sus especies descendientes
(ver aggregates.py), así que los nodos se escriben al final.

    python pipeline/04_export.py
"""
import duckdb

import aggregates
import gbif_extras as gbif
from config import ENRICHED, IMPORT_DIR, REPO, TAXA, SYNONYMS

# neo4j-admin parte las listas (y las etiquetas) por este carácter; uno que
# ningún nombre trae. El import se corre con --array-delimiter=U+001F.
SEP = chr(31)


def enriched(name):
    return f"read_parquet('{(ENRICHED / f'{name}.parquet').as_posix()}')"


OWN = "(TRY_CAST(source_key AS BIGINT) = taxonID)"


def copy(con, sql, name):
    path = (IMPORT_DIR / f"{name}.csv.gz").as_posix()
    con.execute(f"COPY ({sql}) TO '{path}' (HEADER, DELIMITER ',', QUOTE '\"', ESCAPE '\"', "
                "COMPRESSION gzip)")
    rows = con.execute(f"SELECT count(*) FROM read_csv('{path}', all_varchar=true)").fetchone()[0]
    print(f"{name}: {rows:,}", flush=True)


def clean_list(column):
    """Quita el separador de listas de cada elemento antes de unirlos."""
    return f"array_to_string(list_transform({column}, lambda x: replace(x, chr(31), ' ')), chr(31))"


def taxa(con):
    con.execute(f"""
        CREATE TEMP TABLE names AS
        SELECT taxonID, list(DISTINCT name ORDER BY name) AS synonyms
        FROM (SELECT acceptedID AS taxonID, name FROM read_parquet('{SYNONYMS.as_posix()}'))
        GROUP BY taxonID
    """)
    # Un nombre por (taxón, idioma, nombre sin importar mayúsculas).
    con.execute(f"""
        CREATE TEMP TABLE common AS
        SELECT taxonID, list(name ORDER BY language, name) AS names,
               list(language ORDER BY language, name) AS languages
        FROM (
            SELECT taxonID, name, language FROM (
                SELECT taxonID, common_name AS name, language FROM {enriched('wikidata_names')}
                UNION ALL
                SELECT taxonID, vernacular_name, 'es' FROM {enriched('gbif_vernacular_es')}
            )
            WHERE name <> ''
            QUALIFY row_number() OVER (PARTITION BY taxonID, language, lower(name) ORDER BY name) = 1
        )
        GROUP BY taxonID
    """)
    con.execute("INSERT INTO content SELECT taxonID, 'common' FROM common")
    con.execute(f"""
        CREATE TEMP TABLE conservation AS
        SELECT c.taxonID, c.conservation_overall, c.conservation_overall_code
        FROM {enriched('conservation')} c
        LEFT JOIN {gbif.values(gbif.IUCN)} i(status, code, severity)
               ON c.conservation_overall = i.status
        QUALIFY row_number() OVER (PARTITION BY c.taxonID
                                   ORDER BY {OWN.replace('taxonID', 'c.taxonID')} DESC,
                                            i.severity DESC NULLS LAST) = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE habit AS
        SELECT taxonID, habit FROM (
            SELECT taxonID, source_key, habit FROM {enriched('powo')}
            UNION ALL
            SELECT taxonID, source_key, habit FROM {enriched('fishbase')}
        )
        WHERE habit <> ''
        QUALIFY row_number() OVER (PARTITION BY taxonID ORDER BY {OWN} DESC, habit) = 1
    """)
    aggregates.build(con, f"read_parquet('{TAXA.as_posix()}')", "content",
                     "(SELECT taxonID, country_key FROM distribution)",
                     "(SELECT taxonID, conservation_overall_code AS code FROM conservation)")
    copy(con, f"""
        SELECT t.taxonID AS ":ID(Taxon)", t.taxonID AS "taxonID:long", t.name,
               t.scientificName AS scientific_name, t.authorship, t.rank, t.status, t.kingdom,
               {clean_list('n.synonyms')} AS "synonyms:string[]",
               {clean_list('c.names')} AS "commonNames:string[]",
               array_to_string(c.languages, chr(31)) AS "commonNamesLanguages:string[]",
               lower(t.name) AS nameLower,
               k.conservation_overall, k.conservation_overall_code, h.habit,
               coalesce(a.speciesCount, 0) AS "speciesCount:long",
               coalesce(a.withImage, 0) AS "withImage:long",
               coalesce(a.withSound, 0) AS "withSound:long",
               coalesce(a.withText, 0) AS "withText:long",
               coalesce(a.withCommonName, 0) AS "withCommonName:long",
               coalesce(a.withContent, 0) AS "withContent:long",
               array_to_string(a.countries, chr(31)) AS "countries:string[]",
               array_to_string(a.iucnCodes, chr(31)) AS "iucnCodes:string[]",
               array_to_string(a.iucnCounts, chr(31)) AS "iucnCounts:long[]",
               'Taxon' || chr(31) || upper(t.rank[1]) || t.rank[2:] AS ":LABEL"
        FROM read_parquet('{TAXA.as_posix()}') t
        LEFT JOIN names n USING (taxonID)
        LEFT JOIN common c USING (taxonID)
        LEFT JOIN conservation k USING (taxonID)
        LEFT JOIN habit h USING (taxonID)
        LEFT JOIN agg a USING (taxonID)
    """, "taxa")
    copy(con, f"""
        SELECT parentID AS ":START_ID(Taxon)", taxonID AS ":END_ID(Taxon)", 'HAS_CHILD' AS ":TYPE"
        FROM read_parquet('{TAXA.as_posix()}') WHERE parentID IS NOT NULL
    """, "rel_has_child")


def texts(con):
    """Description: un archivo de nodos y uno de relaciones por fuente."""
    sources = {
        # Wikipedia: un artículo por (taxón, idioma); gana el casado por ID y luego el propio.
        "wikipedia": f"""
            SELECT taxonID, text, 'summary' AS type, lang, source_name, source_url
            FROM {enriched('wikipedia')}
            QUALIFY row_number() OVER (PARTITION BY taxonID, lang
                                       ORDER BY method = 'id' DESC, {OWN} DESC,
                                                length(text) DESC) = 1""",
        "fishbase": f"""SELECT taxonID, text, 'description', lang, source_name, source_url
                        FROM {enriched('fishbase')} WHERE text <> ''""",
        "gbif_descriptions": f"""SELECT taxonID, text, type, language, source_name, NULL
                                 FROM {enriched('gbif_descriptions')}""",
        "gbif_references": f"""SELECT taxonID, text, 'reference', NULL, source_name, NULL
                               FROM {enriched('gbif_references')}""",
        "gbif_types": f"""SELECT taxonID, text, 'type_specimen', NULL, source_name, NULL
                          FROM {enriched('gbif_types')}""",
    }
    for name, sql in sources.items():
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE d AS
            SELECT '{name}:' || row_number() OVER () AS id, * FROM (
                SELECT * FROM ({sql}) s(taxonID, text, type, lang, source_name, source_url)
                QUALIFY row_number() OVER (PARTITION BY taxonID, text ORDER BY type) = 1
            )
        """)
        con.execute("""INSERT INTO content SELECT DISTINCT taxonID, 'text' FROM d
                       WHERE type NOT IN ('reference', 'type_specimen')""")
        copy(con, """SELECT id AS ":ID(Description)", text, type, lang, source_name, source_url,
                     'Description' AS ":LABEL" FROM d""", f"description_{name}")
        copy(con, """SELECT taxonID AS ":START_ID(Taxon)", id AS ":END_ID(Description)",
                     'HAS_DESCRIPTION' AS ":TYPE" FROM d""", f"rel_description_{name}")


def media(con):
    sources = {
        "wikimedia": f"""SELECT taxonID, url, media_type, NULL, NULL, license, source_name,
                         source_url FROM {enriched('wikimedia')}""",
        "gbif_images": f"""SELECT taxonID, url, 'image', title, creator, license, source_name,
                           NULL FROM {enriched('gbif_images')}""",
        "xeno_canto": f"""SELECT taxonID, url, media_type, NULL, NULL, license, source_name,
                          source_url FROM {enriched('xeno_canto')}""",
    }
    for name, sql in sources.items():
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE m AS
            SELECT '{name}:' || row_number() OVER () AS id, * FROM (
                SELECT * FROM ({sql})
                     s(taxonID, url, media_type, title, creator, license, source_name, source_url)
                QUALIFY row_number() OVER (PARTITION BY taxonID, url ORDER BY source_name) = 1
            )
        """)
        con.execute("INSERT INTO content SELECT DISTINCT taxonID, media_type FROM m")
        copy(con, """SELECT id AS ":ID(Media)", url, media_type, title, creator, license,
                     source_name, source_url, 'Media' AS ":LABEL" FROM m""", f"media_{name}")
        copy(con, """SELECT taxonID AS ":START_ID(Taxon)", id AS ":END_ID(Media)",
                     'HAS_MEDIA' AS ":TYPE" FROM m""", f"rel_media_{name}")


def geography(con):
    mapping = (REPO / "pipeline" / "country_continent.csv").as_posix()
    con.execute(f"""
        CREATE TEMP TABLE distribution AS
        SELECT d.* FROM {enriched('distribution')} d
        LEFT JOIN {gbif.values(gbif.ESTABLISHMENT)} e(means, priority)
               ON d.establishment_means = e.means
        LEFT JOIN {gbif.values(gbif.IUCN)} i(status, code, severity)
               ON d.conservation_status = i.status
        QUALIFY row_number() OVER (PARTITION BY d.taxonID, d.country_key
                                   ORDER BY {OWN.replace('taxonID', 'd.taxonID')} DESC,
                                            e.priority DESC NULLS LAST,
                                            i.severity DESC NULLS LAST) = 1
    """)
    con.execute(f"""
        CREATE TEMP TABLE countries AS
        SELECT DISTINCT d.country_key, coalesce(m.continent, 'Unknown') AS continent
        FROM distribution d
        LEFT JOIN read_csv('{mapping}', header=true) m USING (country_key)
    """)
    copy(con, """SELECT country_key AS ":ID(Country)", country_key AS key, country_key AS name,
                 'Country' AS ":LABEL" FROM countries""", "countries")
    copy(con, """SELECT DISTINCT continent AS ":ID(Continent)", continent AS name,
                 'Continent' AS ":LABEL" FROM countries""", "continents")
    copy(con, """SELECT country_key AS ":START_ID(Country)", continent AS ":END_ID(Continent)",
                 'PART_OF' AS ":TYPE" FROM countries""", "rel_part_of")
    copy(con, """SELECT taxonID AS ":START_ID(Taxon)", country_key AS ":END_ID(Country)",
                 establishment_means, occurrence_status, conservation_status, conservation_code,
                 'FOUND_IN' AS ":TYPE" FROM distribution""", "rel_found_in")


def main():
    IMPORT_DIR.mkdir(parents=True, exist_ok=True)
    for old in IMPORT_DIR.glob("*.csv.gz"):
        old.unlink()
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{(IMPORT_DIR / '.tmp').as_posix()}'")
    # Qué taxones tienen cada tipo de contenido (image, sound, text, common).
    con.execute("CREATE TEMP TABLE content(taxonID BIGINT, kind VARCHAR)")
    texts(con)
    media(con)
    geography(con)
    taxa(con)
    print(f"Escrito en {IMPORT_DIR}")


if __name__ == "__main__":
    main()
