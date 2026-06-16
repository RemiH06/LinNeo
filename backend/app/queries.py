"""Queries Cypher de LinNeo, separadas de la logica de la API."""

from .db import run_query

# Etiqueta Neo4j y propiedad-key por rango taxonomico
RANK_LABEL = {
    "kingdom": ("Kingdom", "kingdom_key"),
    "phylum":  ("Phylum",  "phylum_key"),
    "class":   ("Class",   "class_key"),
    "order":   ("Order",   "order_key"),
    "family":  ("Family",  "family_key"),
    "genus":   ("Genus",   "genus_key"),
    "species": ("Species", "species_key"),
}
# rango hijo inmediato
CHILD_RANK = {
    "kingdom": "phylum", "phylum": "class", "class": "order",
    "order": "family", "family": "genus", "genus": "species",
}


def search_by_name(term: str, limit: int = 25):
    cypher = """
    CALL db.index.fulltext.queryNodes('species_names', $q)
    YIELD node AS s, score
    RETURN s.species_key AS species_key,
           s.scientific_name AS scientific_name,
           s.canonical_name AS canonical_name,
           s.commonNames AS common_names,
           s.kingdom AS kingdom,
           score
    ORDER BY score DESC
    LIMIT $limit
    """
    q = f"{term}~"
    return run_query(cypher, {"q": q, "limit": limit})


def search_in_descriptions(term: str, limit: int = 25):
    cypher = """
    CALL db.index.fulltext.queryNodes('description_text', $q)
    YIELD node AS d, score
    MATCH (s:Species)-[:HAS_DESCRIPTION]->(d)
    RETURN DISTINCT s.species_key AS species_key,
           s.scientific_name AS scientific_name,
           s.canonical_name AS canonical_name,
           s.kingdom AS kingdom,
           left(d.text, 200) AS snippet,
           score
    ORDER BY score DESC
    LIMIT $limit
    """
    return run_query(cypher, {"q": term, "limit": limit})


def get_species_detail(species_key: int):
    """Ficha completa de una especie, con distribucion enriquecida y conservacion."""
    cypher = """
    MATCH (s:Species {species_key: $key})
    OPTIONAL MATCH (s)-[:HAS_DESCRIPTION]->(d:Description)
    OPTIONAL MATCH (s)-[:HAS_MEDIA]->(m:Media)
    OPTIONAL MATCH (s)-[f:FOUND_IN]->(c:Country)
    OPTIONAL MATCH (c)-[:PART_OF]->(cont:Continent)
    RETURN s.species_key AS species_key,
           s.scientific_name AS scientific_name,
           s.canonical_name AS canonical_name,
           s.commonNames AS common_names,
           s.commonNamesLanguages AS common_names_languages,
           s.kingdom AS kingdom,
           s.habit AS habit,
           s.conservation_overall AS conservation_overall,
           s.conservation_overall_code AS conservation_overall_code,
           collect(DISTINCT {text: d.text, lang: d.lang, type: d.type, source: d.source_name, url: d.source_url, origin: d.origin}) AS descriptions,
           collect(DISTINCT {type: m.media_type, url: m.url, source: m.source_name, source_url: m.source_url, license: m.license}) AS media,
           collect(DISTINCT {
               country: c.key,
               establishment_means: f.establishment_means,
               occurrence_status: f.occurrence_status,
               conservation_status: f.conservation_status,
               conservation_code: f.conservation_code
           }) AS distribution,
           collect(DISTINCT cont.name) AS continents
    """
    rows = run_query(cypher, {"key": species_key})
    if not rows:
        return None
    row = rows[0]
    row["descriptions"] = [d for d in row["descriptions"] if d.get("text")]
    row["media"] = [m for m in row["media"] if m.get("url")]
    row["distribution"] = [d for d in row["distribution"] if d.get("country")]
    # compat: lista simple de codigos para quien la use
    row["countries"] = [d["country"] for d in row["distribution"]]
    return row


def get_taxonomy_path(species_key: int):
    """Linaje Kingdom->Species, con rank, nombre y key de cada nodo (para navegar)."""
    cypher = """
    MATCH (s:Species {species_key: $key})
    OPTIONAL MATCH path = (k:Kingdom)-[:HAS_CHILD*]->(s)
    WITH nodes(path) AS chain
    RETURN [n IN chain | {
        rank: toLower(labels(n)[0]),
        name: coalesce(n.canonical_name, n.name, n.scientific_name),
        key: coalesce(n.kingdom_key, n.phylum_key, n.class_key, n.order_key, n.family_key, n.genus_key, n.species_key)
    }] AS lineage
    LIMIT 1
    """
    rows = run_query(cypher, {"key": species_key})
    return rows[0]["lineage"] if rows and rows[0]["lineage"] else []


def get_relatives(species_key: int, max_per_group: int = 40):
    """Familia, generos hermanos y especies hermanas, con sus keys para navegar."""
    cypher = """
    MATCH (g:Genus)-[:HAS_CHILD]->(s:Species {species_key: $key})
    OPTIONAL MATCH (f:Family)-[:HAS_CHILD]->(g)
    OPTIONAL MATCH (g)-[:HAS_CHILD]->(sib:Species)
      WHERE sib.species_key <> $key
    WITH g, f, collect(DISTINCT {
        name: coalesce(sib.canonical_name, sib.scientific_name),
        key: sib.species_key, rank: 'species'
    })[0..$lim] AS sibling_species
    OPTIONAL MATCH (f)-[:HAS_CHILD]->(sg:Genus)
      WHERE sg <> g
    RETURN
      CASE WHEN f IS NOT NULL THEN {name: coalesce(f.canonical_name, f.name), key: f.family_key, rank: 'family'} ELSE null END AS family,
      {name: coalesce(g.canonical_name, g.name), key: g.genus_key, rank: 'genus'} AS genus,
      collect(DISTINCT {name: coalesce(sg.canonical_name, sg.name), key: sg.genus_key, rank: 'genus'})[0..$lim] AS sibling_genera,
      sibling_species
    LIMIT 1
    """
    rows = run_query(cypher, {"key": species_key, "lim": max_per_group})
    if not rows:
        return {}
    row = rows[0]
    row["sibling_species"] = [x for x in (row.get("sibling_species") or []) if x.get("name")]
    row["sibling_genera"] = [x for x in (row.get("sibling_genera") or []) if x.get("name")]
    return row


def get_taxon_node(rank: str, key: int, child_limit: int = 500):
    """
    Vista de un nodo taxonomico no-especie (genero, familia, orden...):
      - info del nodo (nombre, rank)
      - hijos directos con flags de contenido (imagenes, sonidos, descripcion, etimologia)
      - paises agregados de TODAS las especies descendientes (para el mapa)
    """
    rank = rank.lower()
    if rank not in RANK_LABEL or rank == "species":
        return None
    label, keyprop = RANK_LABEL[rank]
    child_rank = CHILD_RANK.get(rank)
    child_label, child_keyprop = RANK_LABEL[child_rank]

    # Info del nodo
    name_cypher = f"""
    MATCH (n:{label} {{{keyprop}: $key}})
    RETURN coalesce(n.canonical_name, n.name, n.scientific_name) AS name
    LIMIT 1
    """
    nrows = run_query(name_cypher, {"key": key})
    if not nrows:
        return None

    # Hijos directos + flags de contenido.
    # Si el hijo es especie, miramos su propio contenido; si es un taxon superior,
    # agregamos el contenido de sus especies descendientes (EXISTS, barato).
    if child_rank == "species":
        children_cypher = f"""
        MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD]->(c:Species)
        WITH c
        ORDER BY coalesce(c.canonical_name, c.scientific_name)
        LIMIT $climit
        OPTIONAL MATCH (c)-[:HAS_MEDIA]->(mi:Media) WHERE mi.media_type = 'image'
        OPTIONAL MATCH (c)-[:HAS_MEDIA]->(ms:Media) WHERE ms.media_type = 'sound'
        OPTIONAL MATCH (c)-[:HAS_DESCRIPTION]->(d:Description)
        OPTIONAL MATCH (c)-[:HAS_DESCRIPTION]->(et:Description) WHERE et.type = 'etymology'
        OPTIONAL MATCH (c)-[:FOUND_IN]->(co:Country)
        WITH c,
             count(DISTINCT mi) AS n_img,
             count(DISTINCT ms) AS n_snd,
             count(DISTINCT d)  AS n_desc,
             count(DISTINCT et) AS n_etym,
             count(DISTINCT co) AS n_country
        RETURN collect({{
            name: coalesce(c.canonical_name, c.scientific_name),
            key: c.species_key,
            rank: 'species',
            conservation: c.conservation_overall_code,
            flags: {{images: n_img, sounds: n_snd, descriptions: n_desc, etymology: n_etym, countries: n_country}}
        }}) AS children
        """
    else:
        children_cypher = f"""
        MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD]->(c:{child_label})
        WITH c
        ORDER BY coalesce(c.canonical_name, c.name, c.scientific_name)
        LIMIT $climit
        OPTIONAL MATCH (c)-[:HAS_CHILD*]->(s:Species)
        WITH c,
             count(DISTINCT s) AS n_species,
             count(DISTINCT CASE WHEN EXISTS {{ (s)-[:HAS_MEDIA]->(:Media {{media_type:'image'}}) }} THEN s END) AS sp_img,
             count(DISTINCT CASE WHEN EXISTS {{ (s)-[:HAS_MEDIA]->(:Media {{media_type:'sound'}}) }} THEN s END) AS sp_snd,
             count(DISTINCT CASE WHEN EXISTS {{ (s)-[:HAS_DESCRIPTION]->(:Description) }} THEN s END) AS sp_desc
        RETURN collect({{
            name: coalesce(c.canonical_name, c.name, c.scientific_name),
            key: c.{child_keyprop},
            rank: '{child_rank}',
            flags: {{species: n_species, images: sp_img, sounds: sp_snd, descriptions: sp_desc}}
        }}) AS children
        """
    crows = run_query(children_cypher, {"key": key, "climit": child_limit})
    children = crows[0]["children"] if crows else []
    children = [c for c in children if c.get("name")]

    node = {
        "name": nrows[0]["name"],
        "rank": rank,
        "key": key,
        "child_rank": child_rank,
        "children": children,
    }

    # Paises agregados de las especies descendientes (para el mapa)
    countries_cypher = f"""
    MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD*]->(s:Species)-[:FOUND_IN]->(c:Country)
    RETURN collect(DISTINCT c.key) AS countries, count(DISTINCT s) AS species_count
    """
    crows2 = run_query(countries_cypher, {"key": key})
    node["countries"] = crows2[0]["countries"] if crows2 else []
    node["species_count"] = crows2[0]["species_count"] if crows2 else 0
    return node


def filter_species(kingdom: str = None, country: str = None, habit: str = None, limit: int = 50):
    conditions = []
    params = {"limit": limit}
    base = "MATCH (s:Species)"
    if country:
        base += "-[:FOUND_IN]->(c:Country)"
        conditions.append("c.key = $country")
        params["country"] = country
    if kingdom:
        conditions.append("s.kingdom = $kingdom")
        params["kingdom"] = kingdom
    if habit:
        conditions.append("toLower(s.habit) CONTAINS toLower($habit)")
        params["habit"] = habit
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    cypher = f"""
    {base}
    {where}
    RETURN DISTINCT s.species_key AS species_key,
           s.scientific_name AS scientific_name,
           s.canonical_name AS canonical_name,
           s.commonNames AS common_names,
           s.kingdom AS kingdom,
           s.habit AS habit
    LIMIT $limit
    """
    return run_query(cypher, params)


def stats():
    cypher = """
    MATCH (s:Species)
    OPTIONAL MATCH (s)-[:HAS_DESCRIPTION]->(d:Description)
    OPTIONAL MATCH (s)-[:HAS_MEDIA]->(m:Media)
    RETURN count(DISTINCT s) AS total_species,
           count(DISTINCT CASE WHEN d IS NOT NULL THEN s END) AS with_description,
           count(DISTINCT CASE WHEN m.media_type = 'image' THEN s END) AS with_image,
           count(DISTINCT CASE WHEN m.media_type = 'sound' THEN s END) AS with_sound
    """
    rows = run_query(cypher)
    return rows[0] if rows else {}