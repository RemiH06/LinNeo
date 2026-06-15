"""Queries Cypher de LinNeo, separadas de la logica de la API."""

from .db import run_query


def search_by_name(term: str, limit: int = 25):
    """
    Busca por nombre cientifico/canonico (full-text) y por nombre comun.
    Devuelve resultados ligeros para la lista.
    """
    # Full-text sobre nombres cientificos. El term se pasa con comodin.
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
    q = f"{term}~"  # busqueda aproximada (fuzzy)
    return run_query(cypher, {"q": q, "limit": limit})


def search_in_descriptions(term: str, limit: int = 25):
    """Busca un termino dentro de las descripciones."""
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
    """Ficha completa de una especie."""
    cypher = """
    MATCH (s:Species {species_key: $key})
    OPTIONAL MATCH (s)-[:HAS_DESCRIPTION]->(d:Description)
    OPTIONAL MATCH (s)-[:HAS_MEDIA]->(m:Media)
    OPTIONAL MATCH (s)-[:FOUND_IN]->(c:Country)
    OPTIONAL MATCH (c)-[:PART_OF]->(cont:Continent)
    RETURN s.species_key AS species_key,
           s.scientific_name AS scientific_name,
           s.canonical_name AS canonical_name,
           s.commonNames AS common_names,
           s.commonNamesLanguages AS common_names_languages,
           s.kingdom AS kingdom,
           s.habit AS habit,
           collect(DISTINCT {text: d.text, lang: d.lang, source: d.source_name, url: d.source_url}) AS descriptions,
           collect(DISTINCT {type: m.media_type, url: m.url, source: m.source_name, source_url: m.source_url, license: m.license}) AS media,
           collect(DISTINCT c.name) AS countries,
           collect(DISTINCT cont.name) AS continents
    """
    rows = run_query(cypher, {"key": species_key})
    if not rows:
        return None
    row = rows[0]
    # Limpiar listas vacias (descripciones/media nulos)
    row["descriptions"] = [d for d in row["descriptions"] if d.get("text")]
    row["media"] = [m for m in row["media"] if m.get("url")]
    return row


def get_taxonomy_path(species_key: int):
    """Devuelve la jerarquia taxonomica de una especie (Kingdom -> Species)."""
    cypher = """
    MATCH (s:Species {species_key: $key})
    OPTIONAL MATCH path = (k:Kingdom)-[:HAS_CHILD*]->(s)
    WITH s, nodes(path) AS chain
    RETURN [n IN chain | {rank: labels(n)[0], name: coalesce(n.canonical_name, n.scientific_name, n.name)}] AS lineage
    LIMIT 1
    """
    rows = run_query(cypher, {"key": species_key})
    return rows[0]["lineage"] if rows else []


def filter_species(kingdom: str = None, country: str = None, habit: str = None, limit: int = 50):
    """Filtra especies por reino, pais y/o habito."""
    conditions = []
    params = {"limit": limit}

    base = "MATCH (s:Species)"
    if country:
        base += "-[:FOUND_IN]->(c:Country)"
        conditions.append("toLower(c.name) = toLower($country)")
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
    """Estadisticas generales del grafo."""
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