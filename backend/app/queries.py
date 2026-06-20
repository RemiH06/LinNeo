"""Queries Cypher de LinNeo, separadas de la logica de la API."""

import random
from collections import Counter

from .db import run_query

# Etiqueta Neo4j y propiedad-key por rango taxonomico
RANK_LABEL = {
    "domain":  ("Domain",  "domain_key"),
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
    "domain": "kingdom", "kingdom": "phylum", "phylum": "class", "class": "order",
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


def search_clades_by_rank(term: str, rank: str, limit_per_group: int = 100, mode: str = "contains"):
    """
    Busca `term` en UN solo rango taxonomico, agrupado por reino. Para
    species: prioriza los que tienen nombre comun y agrega flags de
    contenido (imagenes/sonidos/descripciones/nombre comun), igual patron
    que get_taxon_node, para que las tarjetas de resultados puedan mostrar
    sellos sin pedir cada especie por separado.
    Para los demas rangos NO se agregan flags (queries mas ligeras: recorrer
    HAS_CHILD* de todos los descendientes seria costoso y no se pidio).

    mode:
      'contains' -- el nombre contiene `term` en cualquier posicion (default)
      'starts'   -- el nombre EMPIEZA con `term`. Para species, el genero es
                    siempre la primera palabra del nombre cientifico, asi que
                    se evalua sobre la SEGUNDA palabra (epiteto), igual patron
                    que el filtro alfabetico de TaxonNode. Para los demas
                    rangos se evalua el nombre completo.
    """
    t = term.strip().lower()
    if not t or rank not in RANK_LABEL:
        return []
    label, keyprop = RANK_LABEL[rank]
    starts = mode == "starts"

    if rank == "species":
        # split(name,' ')[1] = epiteto (2da palabra); si el nombre solo tiene
        # una palabra (raro, pero posible con datos sucios), usar esa misma.
        match_clause = (
            "WITH n, CASE WHEN size(split(coalesce(n.canonical_name, n.scientific_name, ''), ' ')) > 1 "
            "THEN split(coalesce(n.canonical_name, n.scientific_name, ''), ' ')[1] "
            "ELSE coalesce(n.canonical_name, n.scientific_name, '') END AS epithet "
            "WHERE toLower(epithet) STARTS WITH $t"
            if starts else
            "WHERE toLower(coalesce(n.canonical_name, n.scientific_name, '')) CONTAINS $t"
        )
        cypher = f"""
        MATCH (n:{label})
        {match_clause}
        OPTIONAL MATCH (n)-[:HAS_MEDIA]->(mi:Media) WHERE mi.media_type = 'image'
        OPTIONAL MATCH (n)-[:HAS_MEDIA]->(ms:Media) WHERE ms.media_type = 'sound'
        OPTIONAL MATCH (n)-[:HAS_DESCRIPTION]->(d:Description)
        WITH n, count(DISTINCT mi) AS n_img, count(DISTINCT ms) AS n_snd, count(DISTINCT d) AS n_desc,
             size(coalesce(n.commonNames, [])) > 0 AS has_common
        ORDER BY has_common DESC, coalesce(n.canonical_name, n.scientific_name)
        WITH n.kingdom AS kingdom, collect({{
            name: coalesce(n.canonical_name, n.scientific_name),
            key: n.{keyprop},
            rank: '{rank}',
            kingdom: n.kingdom,
            common_names: n.commonNames,
            flags: {{images: n_img, sounds: n_snd, descriptions: n_desc}}
        }})[0..{limit_per_group}] AS items
        WHERE size(items) > 0
        RETURN kingdom, items
        """
    else:
        group_expr = "n.name" if rank == "kingdom" else "n.kingdom"
        name_expr = "coalesce(n.canonical_name, n.name, n.scientific_name, '')"
        where_clause = (
            f"WHERE toLower({name_expr}) STARTS WITH $t" if starts
            else f"WHERE toLower({name_expr}) CONTAINS $t"
        )
        cypher = f"""
        MATCH (n:{label})
        {where_clause}
        WITH n
        ORDER BY coalesce(n.canonical_name, n.name, n.scientific_name)
        WITH {group_expr} AS kingdom, collect({{
            name: coalesce(n.canonical_name, n.name, n.scientific_name),
            key: n.{keyprop},
            rank: '{rank}',
            kingdom: {group_expr}
        }})[0..{limit_per_group}] AS items
        WHERE size(items) > 0
        RETURN kingdom, items
        """
    return run_query(cypher, {"t": t})


def search_clades(term: str, limit_per_group: int = 100, mode: str = "contains"):
    """
    Busca `term` en TODOS los rangos a la vez (usado por callers que prefieren
    una sola respuesta consolidada). Internamente llama a
    search_clades_by_rank por cada rango. El endpoint /search/clades/{rank}
    expone la version per-rank para busquedas progresivas desde el frontend.
    """
    t = term.strip().lower()
    if not t:
        return {}
    results = {}
    for rank in RANK_LABEL:
        rows = search_clades_by_rank(term, rank, limit_per_group, mode)
        if rows:
            results[rank] = rows
    return results


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
    """Linaje Domain->Kingdom->...->Species, con rank, nombre y key de cada nodo (para navegar)."""
    cypher = """
    MATCH (s:Species {species_key: $key})
    OPTIONAL MATCH path = (d:Domain)-[:HAS_CHILD*]->(s)
    WITH nodes(path) AS chain
    RETURN [n IN chain | {
        rank: toLower(labels(n)[0]),
        name: coalesce(n.canonical_name, n.name, n.scientific_name),
        key: coalesce(n.domain_key, n.kingdom_key, n.phylum_key, n.class_key, n.order_key, n.family_key, n.genus_key, n.species_key)
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
             count(DISTINCT co) AS n_country,
             head(collect(DISTINCT mi.url)) AS image,
             collect(DISTINCT co.key) AS countries
        RETURN collect({{
            name: coalesce(c.canonical_name, c.scientific_name),
            key: c.species_key,
            rank: 'species',
            conservation: c.conservation_overall_code,
            image: image,
            common_names: c.commonNames,
            countries: countries,
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
             count(DISTINCT CASE WHEN EXISTS {{ (s)-[:HAS_DESCRIPTION]->(:Description) }} THEN s END) AS sp_desc,
             count(DISTINCT CASE WHEN size(coalesce(s.commonNames, [])) > 0 THEN s END) AS sp_common
        RETURN collect({{
            name: coalesce(c.canonical_name, c.name, c.scientific_name),
            key: c.{child_keyprop},
            rank: '{child_rank}',
            flags: {{species: n_species, images: sp_img, sounds: sp_snd, descriptions: sp_desc, common_names: sp_common}}
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
    Grafo inicial de shui: nodo virtual 'Biota' -> dominios -> reinos.
    Cada nodo lleva su 'kingdom' para colorearse (Domain no se tinta de un solo
    kingdom en frontend si agrupa varios -- eso lo resuelve ShuiGraph con el
    patron de 'canicas' usando domainColors()). Biota es virtual (key null).
    """
    nodes = [{"id": "biota", "name": "Biota", "rank": "root", "key": None, "kingdom": None}]
    links = []
    domains = run_query("""
        MATCH (d:Domain)
        OPTIONAL MATCH (d)-[:HAS_CHILD]->(k:Kingdom)
        WITH d, k ORDER BY coalesce(k.canonical_name, k.name)
        RETURN d.domain_key AS dkey, coalesce(d.canonical_name, d.name) AS dname,
               collect(DISTINCT {key: k.kingdom_key, name: coalesce(k.canonical_name, k.name)}) AS kingdoms
    """)
    for d in domains:
        did = f"domain:{d['dkey']}"
        nodes.append({"id": did, "name": d["dname"], "rank": "domain",
                      "key": d["dkey"], "kingdom": None})
        links.append({"source": "biota", "target": did})
        for k in d["kingdoms"]:
            if not k.get("name"):
                continue
            kid = f"kingdom:{k['key']}"
            nodes.append({"id": kid, "name": k["name"], "rank": "kingdom",
                          "key": k["key"], "kingdom": k["name"]})
            links.append({"source": did, "target": kid})
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

    # reino del nodo (para tintar). El nodo Kingdom no tiene una propiedad propia
    # 'kingdom' en Neo4j (esa propiedad esta denormalizada solo en Species/Genus
    # para queries rapidas; Phylum/Class/Order/Family NO la tienen) -- si el rank
    # enfocado es justo 'kingdom', su propio nombre ES el reino de tinte (igual
    # patron que graph_default). Para cualquier otro rango sin la propiedad
    # propia, se sube por HAS_CHILD hasta el Kingdom ancestro real. Domain es
    # caso aparte, no se tinta un solo color (ver mas abajo).
    krow = run_query(f"""
        MATCH (n:{label} {{{keyprop}: $key}})
        RETURN coalesce(n.canonical_name, n.name, n.scientific_name) AS name, n.kingdom AS kingdom
        LIMIT 1
    """, {"key": key})
    if not krow:
        return None
    center_name = krow[0]["name"]
    if rank == "kingdom":
        kingdom = center_name
    elif rank == "domain":
        kingdom = None
    elif krow[0]["kingdom"]:
        kingdom = krow[0]["kingdom"]
    else:
        # fallback: subir por HAS_CHILD hasta el Kingdom ancestro
        anc_rows = run_query(f"""
            MATCH (k:Kingdom)-[:HAS_CHILD*]->(n:{label} {{{keyprop}: $key}})
            RETURN coalesce(k.canonical_name, k.name) AS kname
            LIMIT 1
        """, {"key": key})
        kingdom = anc_rows[0]["kname"] if anc_rows else None

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

    # nivel 1: ordenado por cantidad de hijos propios (descendientes directos),
    # no alfabetico -- asi las ramas con mas contenido (ej. tras reconectar
    # huerfanos de Fungi) siempre entran en el cap de 60, en vez de perderse
    # por orden alfabetico frente a ramas pequenas que empiezan con A.
    if chain:
        lvl1_label, lvl1_keyprop = RANK_LABEL[chain[0]]
        lvl1 = []
        if len(chain) > 1:
            lvl2_label, lvl2_keyprop = RANK_LABEL[chain[1]]
            lvl1 = run_query(f"""
                MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD]->(c:{lvl1_label})
                OPTIONAL MATCH (c)-[:HAS_CHILD]->(gc:{lvl2_label})
                WITH c, count(DISTINCT gc) AS n_children
                RETURN c.{lvl1_keyprop} AS key, coalesce(c.canonical_name, c.name, c.scientific_name) AS name, n_children
                ORDER BY n_children DESC, name LIMIT 60
            """, {"key": key})
        else:
            # rank ya es el ultimo nivel con hijos (ej. genus -> species, sin
            # nivel 2 despues); no hay nietos que contar, alfabetico esta bien
            lvl1 = run_query(f"""
                MATCH (n:{label} {{{keyprop}: $key}})-[:HAS_CHILD]->(c:{lvl1_label})
                RETURN c.{lvl1_keyprop} AS key, coalesce(c.canonical_name, c.name, c.scientific_name) AS name
                ORDER BY name LIMIT 60
            """, {"key": key})
        for c in lvl1:
            if not c.get("name"):
                continue
            cid = f"{chain[0]}:{c['key']}"
            # Si los hijos directos son Kingdom (centro = Domain), cada uno se tinta
            # de SU PROPIO nombre, no del padre (un Domain agrupa varios reinos,
            # no tiene un solo color uniforme que heredar).
            child_kingdom = c["name"] if chain[0] == "kingdom" else kingdom
            if cid not in seen:
                seen.add(cid)
                nodes.append({"id": cid, "name": c["name"], "rank": chain[0], "key": c["key"], "kingdom": child_kingdom})
            links.append({"source": center_id, "target": cid})
        # nivel 2: CUPO FIJO de nietos por cada hermano de nivel 1 (no un LIMIT
        # global), asi ningun hermano se queda en cero aunque su vecino tenga
        # miles de hijos. 15 por hermano x hasta 60 hermanos = techo teorico
        # de 900 nodos, pero en la practica casi siempre es mucho menos.
        NIETOS_POR_HERMANO = 15
        if len(chain) > 1:
            lvl2_label, lvl2_keyprop = RANK_LABEL[chain[1]]
            lvl1_keys = [c["key"] for c in lvl1 if c.get("key") is not None]
            lvl1_kingdom_by_key = {c["key"]: c["name"] for c in lvl1} if chain[0] == "kingdom" else None
            if lvl1_keys:
                lvl2 = run_query(f"""
                    MATCH (p:{lvl1_label})-[:HAS_CHILD]->(c:{lvl2_label})
                    WHERE p.{lvl1_keyprop} IN $pkeys
                    WITH p, c
                    ORDER BY coalesce(c.canonical_name, c.name, c.scientific_name)
                    WITH p, collect({{
                        key: c.{lvl2_keyprop},
                        name: coalesce(c.canonical_name, c.name, c.scientific_name)
                    }})[0..$cupo] AS kids
                    RETURN p.{lvl1_keyprop} AS pkey, kids
                """, {"pkeys": lvl1_keys, "cupo": NIETOS_POR_HERMANO})
                for row in lvl2:
                    pid = f"{chain[0]}:{row['pkey']}"
                    for c in row["kids"]:
                        if not c.get("name"):
                            continue
                        cid = f"{chain[1]}:{c['key']}"
                        # Mismo criterio: si el padre directo es Kingdom, el nieto hereda
                        # el nombre de ESE kingdom padre (no el kingdom global del centro).
                        grandchild_kingdom = lvl1_kingdom_by_key.get(row["pkey"], kingdom) if lvl1_kingdom_by_key else kingdom
                        if cid not in seen:
                            seen.add(cid)
                            nodes.append({"id": cid, "name": c["name"], "rank": chain[1], "key": c["key"], "kingdom": grandchild_kingdom})
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


def random_from_kingdom_pool(kingdoms: list, n: int = 8):
    """
    'Pot' de reinos activos: hace `n` tiradas independientes, cada una elige
    un reino al azar DENTRO de `kingdoms` (no uno fijo por reino), y dentro de
    ese reino una especie al azar con descripcion. Un mismo reino puede salir
    varias veces o ninguna -- es un sorteo real, no 1 por reino garantizado.
    Si `kingdoms` viene vacio, usa todos los Kingdom existentes como pool.
    El sorteo del reino se hace en Python (random.choice) para no depender de
    sintaxis Cypher de indexacion dinamica de listas; cada tirada resultante
    se agrupa por reino para hacer una sola query por reino distinto que
    salio en el sorteo (no N queries individuales).
    """
    if not kingdoms:
        rows = run_query("MATCH (k:Kingdom) RETURN coalesce(k.canonical_name, k.name) AS name")
        kingdoms = [r["name"] for r in rows if r.get("name")]
    if not kingdoms:
        return []

    draws = [random.choice(kingdoms) for _ in range(n)]
    counts = Counter(draws)

    cypher = """
    UNWIND $picks AS pick
    CALL {
        WITH pick
        MATCH (s:Species)
        WHERE s.kingdom = pick.kingdom AND EXISTS { (s)-[:HAS_DESCRIPTION]->(:Description) }
        WITH s, rand() AS r ORDER BY r LIMIT pick.count
        OPTIONAL MATCH (s)-[m:HAS_MEDIA]->(md:Media) WHERE md.media_type = 'image'
        WITH s, head(collect(md.url)) AS image
        RETURN collect({
            species_key: s.species_key,
            name: coalesce(s.canonical_name, s.scientific_name),
            kingdom: s.kingdom,
            image: image
        }) AS examples
    }
    RETURN examples
    """
    picks = [{"kingdom": k, "count": c} for k, c in counts.items()]
    rows = run_query(cypher, {"picks": picks})
    examples = []
    for r in rows:
        examples.extend(r.get("examples") or [])
    random.shuffle(examples)
    return examples[:n]


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


def get_continent_node(continent: str):
    """
    Vista de un continente: lista de sus paises, cada uno con el numero de
    especies presentes (FOUND_IN) para que la tarjeta muestre algo de
    contexto. No se agregan flags de contenido aqui (seria un agregado de
    TODAS las especies de TODOS los paises, demasiado caro); eso se calcula
    al entrar a un pais especifico via get_country_node.
    """
    cypher = """
    MATCH (co:Country)-[:PART_OF]->(cont:Continent {name: $cont})
    OPTIONAL MATCH (s:Species)-[:FOUND_IN]->(co)
    WITH co, count(DISTINCT s) AS n_species
    ORDER BY co.name
    RETURN collect({
        name: co.name,
        key: co.key,
        rank: 'country',
        species_count: n_species
    }) AS countries
    """
    rows = run_query(cypher, {"cont": continent})
    if not rows:
        return None
    return {"name": continent, "rank": "continent", "countries": rows[0]["countries"]}


def get_country_node(country_code: str, child_limit: int = 500):
    """
    Vista de un pais: especies presentes (FOUND_IN), con el mismo shape de
    flags/image/common_names que get_taxon_node, para que la lista de
    resultados se sienta igual que la de un taxon. Tambien agrega el desglose
    por reino (cuantas especies de cada kingdom hay en el pais), util para
    mostrar contexto sin tener que abrir cada especie.
    """
    code = (country_code or "").strip().upper()
    if not code:
        return None

    name_cypher = """
    MATCH (co:Country {key: $code})
    OPTIONAL MATCH (co)-[:PART_OF]->(cont:Continent)
    RETURN co.name AS name, cont.name AS continent
    LIMIT 1
    """
    nrows = run_query(name_cypher, {"code": code})
    if not nrows or not nrows[0].get("name"):
        return None

    species_cypher = """
    MATCH (s:Species)-[:FOUND_IN]->(co:Country {key: $code})
    WITH s
    ORDER BY coalesce(s.canonical_name, s.scientific_name)
    LIMIT $climit
    OPTIONAL MATCH (s)-[:HAS_MEDIA]->(mi:Media) WHERE mi.media_type = 'image'
    OPTIONAL MATCH (s)-[:HAS_MEDIA]->(ms:Media) WHERE ms.media_type = 'sound'
    OPTIONAL MATCH (s)-[:HAS_DESCRIPTION]->(d:Description)
    WITH s,
         count(DISTINCT mi) AS n_img,
         count(DISTINCT ms) AS n_snd,
         count(DISTINCT d)  AS n_desc,
         head(collect(DISTINCT mi.url)) AS image
    RETURN collect({
        name: coalesce(s.canonical_name, s.scientific_name),
        key: s.species_key,
        rank: 'species',
        kingdom: s.kingdom,
        conservation: s.conservation_overall_code,
        image: image,
        common_names: s.commonNames,
        flags: {images: n_img, sounds: n_snd, descriptions: n_desc}
    }) AS species
    """
    srows = run_query(species_cypher, {"code": code, "climit": child_limit})
    species = srows[0]["species"] if srows else []

    kingdom_cypher = """
    MATCH (s:Species)-[:FOUND_IN]->(co:Country {key: $code})
    WHERE s.kingdom IS NOT NULL
    RETURN s.kingdom AS kingdom, count(DISTINCT s) AS n
    ORDER BY n DESC
    """
    kingdom_rows = run_query(kingdom_cypher, {"code": code})

    total_cypher = """
    MATCH (s:Species)-[:FOUND_IN]->(co:Country {key: $code})
    RETURN count(DISTINCT s) AS total
    """
    total_rows = run_query(total_cypher, {"code": code})
    total = total_rows[0]["total"] if total_rows else 0

    return {
        "name": nrows[0]["name"],
        "key": code,
        "rank": "country",
        "continent": nrows[0].get("continent"),
        "species_count": total,
        "by_kingdom": kingdom_rows,
        "children": species,
        "child_rank": "species",
    }