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
    RETURN coalesce(n.canonical_name, n.name, n.scientific_name) AS name,
           n.kingdom AS kingdom
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
        "kingdom": nrows[0]["kingdom"],
        "rank": rank,
        "key": key,
        "child_rank": child_rank,
        "children": children,
    }

    # Linaje (ancestros) del nodo, para el grafo de navegacion
    lineage_cypher = f"""
    MATCH (n:{label} {{{keyprop}: $key}})
    OPTIONAL MATCH path = (k:Kingdom)-[:HAS_CHILD*]->(n)
    WITH nodes(path) AS chain
    RETURN [x IN chain | {{
        rank: toLower(labels(x)[0]),
        name: coalesce(x.canonical_name, x.name, x.scientific_name),
        key: coalesce(x.kingdom_key, x.phylum_key, x.class_key, x.order_key, x.family_key, x.genus_key)
    }}] AS lineage
    LIMIT 1
    """
    lrows = run_query(lineage_cypher, {"key": key})
    node["lineage"] = lrows[0]["lineage"] if lrows and lrows[0]["lineage"] else []

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


# ============================================================
# SHUI -- grafo principal, ejemplos por reino, filtros geograficos
# ============================================================

def list_kingdoms():
    """Los reinos con su key, para el grafo y los ejemplos."""
    cypher = """
    MATCH (k:Kingdom)
    RETURN k.kingdom_key AS key, coalesce(k.canonical_name, k.name) AS name
    ORDER BY name
    """
    return run_query(cypher)


def graph_default():
    """
    Grafo inicial de shui: nodo virtual 'Biota' -> reinos -> filos.
    Cada nodo lleva su 'kingdom' para colorearse. Biota es virtual (key null).
    """
    nodes = [{"id": "biota", "name": "Biota", "rank": "root", "key": None, "kingdom": None}]
    links = []
    kingdoms = run_query("""
        MATCH (k:Kingdom)
        OPTIONAL MATCH (k)-[:HAS_CHILD]->(p:Phylum)
        WITH k, p ORDER BY coalesce(p.canonical_name, p.name)
        RETURN k.kingdom_key AS kkey, coalesce(k.canonical_name, k.name) AS kname,
               collect(DISTINCT {key: p.phylum_key, name: coalesce(p.canonical_name, p.name)}) AS phyla
    """)
    for k in kingdoms:
        kid = f"kingdom:{k['kkey']}"
        nodes.append({"id": kid, "name": k["kname"], "rank": "kingdom",
                      "key": k["kkey"], "kingdom": k["kname"]})
        links.append({"source": "biota", "target": kid})
        for p in k["phyla"]:
            if not p.get("name"):
                continue
            pid = f"phylum:{p['key']}"
            nodes.append({"id": pid, "name": p["name"], "rank": "phylum",
                          "key": p["key"], "kingdom": k["kname"]})
            links.append({"source": kid, "target": pid})
    return {"nodes": nodes, "links": links, "center": "biota", "kingdom": None}


def graph_focus(rank: str, key: int, depth: int = 1):
    """
    Grafo centrado en un nodo: el nodo + hasta `depth` niveles de descendientes.
    Todos comparten reino (mismo color, distinta luminosidad por rango).
    """
    rank = rank.lower()
    if rank not in RANK_LABEL:
        return None
    label, keyprop = RANK_LABEL[rank]

    # reino del nodo (para tintar)
    krow = run_query(f"""
        MATCH (n:{label} {{{keyprop}: $key}})
        RETURN coalesce(n.canonical_name, n.name, n.scientific_name) AS name, n.kingdom AS kingdom
        LIMIT 1
    """, {"key": key})
    if not krow:
        return None
    center_name = krow[0]["name"]
    kingdom = krow[0]["kingdom"]

    # cadena de rangos hacia abajo desde `rank`
    chain = []
    r = rank
    for _ in range(depth):
        r = CHILD_RANK.get(r)
        if not r:
            break
        chain.append(r)

    center_id = f"{rank}:{key}"
    nodes = [{"id": center_id, "name": center_name, "rank": rank, "key": key, "kingdom": kingdom}]
    links = []
    seen = {center_id}

    # nivel 1
    if chain:
        lvl1_label, lvl1_keyprop = RANK_LABEL[chain[0]]
        lvl1 = run_query(f"""
            MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD]->(c:{lvl1_label})
            RETURN c.{lvl1_keyprop} AS key, coalesce(c.canonical_name, c.name, c.scientific_name) AS name
            ORDER BY name LIMIT 60
        """, {"key": key})
        for c in lvl1:
            if not c.get("name"):
                continue
            cid = f"{chain[0]}:{c['key']}"
            if cid not in seen:
                seen.add(cid)
                nodes.append({"id": cid, "name": c["name"], "rank": chain[0], "key": c["key"], "kingdom": kingdom})
            links.append({"source": center_id, "target": cid})
        # nivel 2
        if len(chain) > 1:
            lvl2_label, lvl2_keyprop = RANK_LABEL[chain[1]]
            lvl1_keys = [c["key"] for c in lvl1 if c.get("key") is not None]
            if lvl1_keys:
                lvl2 = run_query(f"""
                    MATCH (p:{lvl1_label})-[:HAS_CHILD]->(c:{lvl2_label})
                    WHERE p.{lvl1_keyprop} IN $pkeys
                    RETURN p.{lvl1_keyprop} AS pkey, c.{lvl2_keyprop} AS key,
                           coalesce(c.canonical_name, c.name, c.scientific_name) AS name
                    ORDER BY name LIMIT 300
                """, {"pkeys": lvl1_keys})
                for c in lvl2:
                    if not c.get("name"):
                        continue
                    pid = f"{chain[0]}:{c['pkey']}"
                    cid = f"{chain[1]}:{c['key']}"
                    if cid not in seen:
                        seen.add(cid)
                        nodes.append({"id": cid, "name": c["name"], "rank": chain[1], "key": c["key"], "kingdom": kingdom})
                    links.append({"source": pid, "target": cid})

    return {"nodes": nodes, "links": links, "center": center_id, "kingdom": kingdom}


def random_by_kingdom(per_kingdom: int = 1):
    """
    Una especie aleatoria por reino, que tenga descripcion. Devuelve imagen si la hay.
    Optimizado: en vez de recorrer HAS_CHILD* (carisimo), parte de las Species que
    tienen descripcion, muestrea por reino usando s.kingdom (propiedad directa).
    """
    cypher = """
    MATCH (k:Kingdom)
    WITH coalesce(k.canonical_name, k.name) AS kingdom
    CALL {
        WITH kingdom
        MATCH (s:Species)
        WHERE s.kingdom = kingdom AND EXISTS { (s)-[:HAS_DESCRIPTION]->(:Description) }
        WITH s, rand() AS r ORDER BY r LIMIT $n
        OPTIONAL MATCH (s)-[m:HAS_MEDIA]->(md:Media) WHERE md.media_type = 'image'
        WITH s, head(collect(md.url)) AS image
        RETURN collect({
            species_key: s.species_key,
            name: coalesce(s.canonical_name, s.scientific_name),
            kingdom: s.kingdom,
            image: image
        }) AS examples
    }
    RETURN kingdom, examples
    """
    return run_query(cypher, {"n": per_kingdom})


def random_descendants(rank: str, key: int, n: int = 9):
    """Especies aleatorias con descripcion, descendientes de un nodo (mismo reino)."""
    rank = rank.lower()
    if rank not in RANK_LABEL:
        return []
    label, keyprop = RANK_LABEL[rank]
    cypher = f"""
    MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD*]->(s:Species)
    WHERE EXISTS {{ (s)-[:HAS_DESCRIPTION]->(:Description) }}
    WITH s, rand() AS r ORDER BY r LIMIT $n
    OPTIONAL MATCH (s)-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'image'
    RETURN s.species_key AS species_key,
           coalesce(s.canonical_name, s.scientific_name) AS name,
           s.kingdom AS kingdom,
           head(collect(m.url)) AS image
    """
    return run_query(cypher, {"key": key, "n": n})


def list_continents():
    cypher = "MATCH (c:Continent) RETURN c.name AS name ORDER BY name"
    return [r["name"] for r in run_query(cypher)]


def countries_in_continent(continent: str):
    cypher = """
    MATCH (c:Country)-[:PART_OF]->(k:Continent {name: $cont})
    RETURN c.key AS key, c.name AS name ORDER BY name
    """
    return run_query(cypher, {"cont": continent})