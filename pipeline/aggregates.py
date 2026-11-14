"""Agregados por taxón sobre sus especies descendientes.

La data es estática entre cargas, así que la API lee estos conteos como
propiedades en vez de recorrer HAS_CHILD* en cada consulta (en Animalia cada
recorrido tarda 3 a 4 s). Cada taxón cuenta sus especies descendientes; una
especie se cuenta a sí misma, con conteos de 0 o 1.

Los conteos de un padre son la suma de los de sus hijos: la API muestrea
especies al azar bajando por el árbol con estos conteos como pesos.
"""


def build(con, tree, content, geo, iucn):
    """Crea la tabla temporal `agg` con una fila por taxón que tiene especies.

    tree:    (taxonID, parentID, rank)
    content: (taxonID, kind), kind en image, sound, text, common
    geo:     (taxonID, country_key)
    iucn:    (taxonID, code)
    Cada argumento es una relación SQL (tabla o read_parquet).
    """
    con.execute(f"CREATE OR REPLACE TEMP TABLE agg_tree AS SELECT taxonID, parentID, rank FROM {tree}")
    # (miembro, ancestro), con el propio taxón como su ancestro de nivel 0.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE agg_closure AS
        WITH RECURSIVE up(member, anc) AS (
            SELECT taxonID, taxonID FROM agg_tree
            UNION ALL
            SELECT up.member, t.parentID FROM up JOIN agg_tree t ON t.taxonID = up.anc
            WHERE t.parentID IS NOT NULL
        )
        SELECT * FROM up
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE agg_species_content AS
        SELECT t.taxonID,
               bool_or(c.kind = 'image') IS TRUE AS img,
               bool_or(c.kind = 'sound') IS TRUE AS snd,
               bool_or(c.kind = 'text') IS TRUE AS txt,
               bool_or(c.kind = 'common') IS TRUE AS com
        FROM agg_tree t LEFT JOIN {content} c USING (taxonID)
        WHERE t.rank = 'species'
        GROUP BY t.taxonID
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE agg_counts AS
        SELECT c.anc AS taxonID, count(*) AS speciesCount,
               count(*) FILTER (s.img) AS withImage,
               count(*) FILTER (s.snd) AS withSound,
               count(*) FILTER (s.txt) AS withText,
               count(*) FILTER (s.com) AS withCommonName,
               count(*) FILTER (s.img OR s.snd OR s.txt OR s.com) AS withContent
        FROM agg_closure c JOIN agg_species_content s ON s.taxonID = c.member
        GROUP BY c.anc
    """)
    # Las especies ya tienen FOUND_IN y su propio estado: estas listas son para el resto.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE agg_countries AS
        SELECT c.anc AS taxonID, list(DISTINCT g.country_key ORDER BY g.country_key) AS countries
        FROM agg_closure c
        JOIN {geo} g ON g.taxonID = c.member
        JOIN agg_tree a ON a.taxonID = c.anc AND a.rank <> 'species'
        GROUP BY c.anc
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE agg_iucn_counts AS
        SELECT taxonID, list(code ORDER BY code) AS iucnCodes, list(n ORDER BY code) AS iucnCounts
        FROM (
            SELECT c.anc AS taxonID, i.code, count(*) AS n
            FROM agg_closure c
            JOIN {iucn} i ON i.taxonID = c.member
            JOIN agg_tree m ON m.taxonID = c.member AND m.rank = 'species'
            JOIN agg_tree a ON a.taxonID = c.anc AND a.rank <> 'species'
            WHERE i.code IS NOT NULL
            GROUP BY ALL
        )
        GROUP BY taxonID
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE agg AS
        SELECT * FROM agg_counts
        LEFT JOIN agg_countries USING (taxonID)
        LEFT JOIN agg_iucn_counts USING (taxonID)
    """)
    for table in ("agg_tree", "agg_closure","agg_species_content", "agg_counts", "agg_countries", "agg_iucn_counts"):
        con.execute(f"DROP TABLE {table}")
