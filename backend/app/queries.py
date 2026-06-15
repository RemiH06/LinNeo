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


def get_relatives(species_key: int, max_per_group: int = 40):
    """
    Arbol de parentesco de dos niveles para la especie dada:
    - Familia (ancestro) -> generos hermanos (hijos de la familia)
    - dentro del genero propio -> especies hermanas (hijos del genero)

    Estructura devuelta:
    {
      family: {name, key},
      genus:  {name, key},
      sibling_genera: [{name, key}, ...],   # otros generos de la familia
      sibling_species: [{name, key}, ...],  # otras especies del mismo genero
    }
    """
    cypher = """
    MATCH (g:Genus)-[:HAS_CHILD]->(s:Species {species_key: $key})
    OPTIONAL MATCH (f:Family)-[:HAS_CHILD]->(g)
    // especies hermanas (mismo genero, excluyendo la actual)
    OPTIONAL MATCH (g)-[:HAS_CHILD]->(sib:Species)
      WHERE sib.species_key <> $key
    WITH g, f, collect(DISTINCT {
        name: coalesce(sib.canonical_name, sib.scientific_name),
        key: sib.species_key
    })[0..$lim] AS sibling_species
    // generos hermanos (misma familia, excluyendo el propio)
    OPTIONAL MATCH (f)-[:HAS_CHILD]->(sg:Genus)
      WHERE sg <> g
    RETURN
      CASE WHEN f IS NOT NULL THEN {name: coalesce(f.canonical_name, f.name), key: null} ELSE null END AS family,
      {name: coalesce(g.canonical_name, g.name), key: null} AS genus,
      collect(DISTINCT {name: coalesce(sg.canonical_name, sg.name), key: null})[0..$lim] AS sibling_genera,
      sibling_species
    LIMIT 1
    """
    rows = run_query(cypher, {"key": species_key, "lim": max_per_group})
    if not rows:
        return {}
    row = rows[0]
    # limpiar entradas nulas
    row["sibling_species"] = [x for x in (row.get("sibling_species") or []) if x.get("name")]
    row["sibling_genera"] = [x for x in (row.get("sibling_genera") or []) if x.get("name")]
    return row


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