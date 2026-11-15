"""Consultas Cypher de LinNeo, separadas de las rutas de la API.

Todo se llavea por `taxonID`, siempre con la etiqueta `Taxon`: es la del índice
(`MATCH (s:Species {taxonID: ...})` escanea las 2.6 M especies). Los conteos sobre especies descendientes
(speciesCount, withImage, withSound, withText, withCommonName, withContent,
countries, iucnCodes/iucnCounts) vienen precalculados del pipeline
(pipeline/aggregates.py): ninguna ruta recorre HAS_CHILD* completo.
"""

import random
import re
from functools import lru_cache

from .db import run_query

RANKS = ("domain", "kingdom", "phylum", "class", "order", "family", "genus", "species")
NOT_TEXT = ["reference", "type_specimen"]  # Description que no cuentan como texto
SEARCH_WINDOW = 200  # candidatos de Lucene que se reordenan y paginan

# Flags de una especie: conteos reales de su contenido (sus conteos precalculados son 0 o 1).
SPECIES_FLAGS = """{
    images: COUNT { (%(v)s)-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'image' },
    sounds: COUNT { (%(v)s)-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'sound' },
    descriptions: COUNT { (%(v)s)-[:HAS_DESCRIPTION]->(d:Description) WHERE NOT d.type IN $not_text },
    etymology: COUNT { (%(v)s)-[:HAS_DESCRIPTION]->(:Description {type: 'etymology'}) }
}"""
# Flags de un taxón superior: especies descendientes con cada tipo de contenido.
TAXON_FLAGS = """{
    species: %(v)s.speciesCount, images: %(v)s.withImage, sounds: %(v)s.withSound,
    descriptions: %(v)s.withText, common_names: %(v)s.withCommonName
}"""
FIRST_IMAGE = ("head(COLLECT { MATCH (%(v)s)-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'image' "
               "RETURN m.url LIMIT 1 })")


LANG_ORDER = {"es": 0, "en": 1}  # nombre común preferido: español, luego inglés


def _by_language(names, langs):
    """Nombres comunes con los de español primero y luego los de inglés; el resto, en su orden.

    Sin repetidos: "Jaguar" en inglés, alemán y francés sale una vez.
    """
    if not names:
        return names
    langs = langs or [None] * len(names)
    pairs = sorted(zip(names, langs), key=lambda p: LANG_ORDER.get(p[1], len(LANG_ORDER)))
    seen, out = set(), []
    for n, _ in pairs:
        if n.lower() not in seen:
            seen.add(n.lower()); out.append(n)
    return out


def _f(template, v):
    return template % {"v": v}


def _run(cypher, **params):
    return run_query(cypher, {"not_text": NOT_TEXT, **params})


def _node_id(rank, key):
    return f"{rank}:{key}"


# ── Búsqueda ──

LUCENE_SPECIAL = re.compile(r'([+\-&|!(){}\[\]^"~*?:\\/])')


def lucene_query(term):
    """Cada palabra exacta, por prefijo o con una letra de diferencia; todas obligatorias."""
    words = [LUCENE_SPECIAL.sub(r"\\\1", w) for w in term.lower().split()]
    return " AND ".join(f"({w}^3 OR {w}* OR {w}~1)" for w in words)


def search(term, offset=0, limit=25):
    """Búsqueda por nombre científico, sinónimo o nombre común.

    Lucene castiga los campos con muchos nombres (Panthera onca tiene 13 comunes
    y "jaguar" la dejaba en 4.º), así que se reordenan sus primeros candidatos:
    coincidencia exacta primero, luego cuántos nombres comunes tiene (un taxón
    conocido tiene nombre en más idiomas) y al final el puntaje de Lucene.
    """
    query = lucene_query(term)
    if not query:
        return {"total": 0, "results": []}
    rows = _run("""
        CALL db.index.fulltext.queryNodes('taxon_search', $q, {limit: $window})
        YIELD node AS t, score
        WITH t, score,
             t.nameLower = $exact
             OR any(x IN coalesce(t.commonNames, []) + coalesce(t.synonyms, [])
                    WHERE toLower(x) = $exact) AS exact
        RETURN t.taxonID AS key, t.name AS name, t.scientific_name AS scientific_name,
               t.rank AS rank, t.kingdom AS kingdom, t.commonNames AS common_names,
               t.commonNamesLanguages AS langs,
               // qué sinónimo explica la coincidencia, para decirlo en la interfaz
               head([x IN coalesce(t.synonyms, []) WHERE toLower(x) CONTAINS $exact]) AS synonym_match,
               exact, size(coalesce(t.commonNames, [])) AS richness, score
        ORDER BY exact DESC, richness DESC, score DESC
    """, q=query, window=SEARCH_WINDOW, exact=" ".join(term.lower().split()))
    page = rows[offset:offset + limit]
    # El padre distingue homónimos; solo para la página, no para la ventana completa.
    parents = {r["key"]: r["parent"] for r in _run("""
        UNWIND $keys AS k MATCH (p:Taxon)-[:HAS_CHILD]->(:Taxon {taxonID: k})
        RETURN k AS key, p.name AS parent
    """, keys=[r["key"] for r in page])} if page else {}
    for r in page:
        r["common_names"] = _by_language(r["common_names"], r.pop("langs"))
        r["parent"] = parents.get(r["key"])
    return {"total": len(rows), "results": page}


# La data no cambia mientras la API corre (un import exige reiniciarla), así que
# lo que sigue caro tras los agregados se guarda en memoria. Medido en Pixilate:
# galería de Animalia 2.2 s, subcadena "ana" en especies 0.6 s.
cached = lru_cache(maxsize=256)


@cached
def search_clades_by_rank(term, rank, limit_per_group=100, mode="contains"):
    """Busca `term` como subcadena del nombre en un rango, agrupado por reino.

    mode='starts': el nombre empieza con `term`; en especies, el epíteto (la
    segunda palabra), porque la primera siempre es el género.
    Usa el índice TEXT de nameLower (trigramas).
    """
    t = term.strip().lower()
    if not t or rank not in RANKS:
        return []
    if mode == "starts" and rank == "species":
        where, t = "n.nameLower CONTAINS $t", " " + t
    elif mode == "starts":
        where = "n.nameLower STARTS WITH $t"
    else:
        where = "n.nameLower CONTAINS $t"
    group = "n.name" if rank == "kingdom" else "n.kingdom"
    if rank == "species":
        order = "size(coalesce(n.commonNames, [])) > 0 DESC, n.name"
        extra = f", common_names: n.commonNames, flags: {_f(SPECIES_FLAGS, 'n')}"
    else:
        order = "n.name"
        extra = ""
    return _run(f"""
        MATCH (n:Taxon) USING TEXT INDEX n:Taxon(nameLower)
        WHERE {where} AND n.rank = $rank
        WITH n ORDER BY {order}
        WITH {group} AS kingdom, collect(n)[0..$lim] AS ns
        UNWIND ns AS n
        // El padre distingue homónimos (dos Cepolidae en Animalia) en las sugerencias.
        WITH kingdom, n {{.name, key: n.taxonID, .rank, .kingdom{extra},
                         parent: head(COLLECT {{ MATCH (p:Taxon)-[:HAS_CHILD]->(n) RETURN p.name }})}} AS item
        RETURN kingdom, collect(item) AS items
    """, t=t, rank=rank, lim=limit_per_group)


def search_clades(term, limit_per_group=100, mode="contains"):
    """search_clades_by_rank en todos los rangos, en una sola respuesta."""
    results = {}
    for rank in RANKS:
        rows = search_clades_by_rank(term, rank, limit_per_group, mode)
        if rows:
            results[rank] = rows
    return results


def filter_species(country, kingdom=None, habit=None, offset=0, limit=50):
    """Especies de un país, opcionalmente de un reino o hábito.

    El país es obligatorio: parte del índice de Country y expande sus FOUND_IN.
    Sin él, reino o hábito obligan a escanear las 2.6 M especies (0.7 a 3 s).
    """
    conditions, params = [], {"country": country.upper(), "offset": offset, "limit": limit}
    if kingdom:
        conditions.append("s.kingdom = $kingdom")
        params["kingdom"] = kingdom
    if habit:
        conditions.append("toLower(s.habit) CONTAINS toLower($habit)")
        params["habit"] = habit
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return _run(f"""
        MATCH (s:Species)-[:FOUND_IN]->(:Country {{key: $country}}) {where}
        RETURN s.taxonID AS key, s.name AS name, s.scientific_name AS scientific_name,
               s.commonNames AS common_names, s.kingdom AS kingdom, s.habit AS habit
        ORDER BY s.taxonID SKIP $offset LIMIT $limit
    """, **params)


# ── Especie ──

def species_detail(key):
    rows = _run("""
        MATCH (s:Taxon:Species {taxonID: $key})
        RETURN s.taxonID AS key, s.name AS name, s.scientific_name AS scientific_name,
               s.authorship AS authorship, s.rank AS rank, s.status AS status,
               s.kingdom AS kingdom, s.synonyms AS synonyms,
               s.commonNames AS common_names, s.commonNamesLanguages AS common_names_languages,
               s.habit AS habit, s.conservation_overall AS conservation_overall,
               s.conservation_overall_code AS conservation_overall_code,
               COLLECT { MATCH (s)-[:HAS_DESCRIPTION]->(d)
                         RETURN d {.text, .lang, .type, source: d.source_name, url: d.source_url} }
                   AS descriptions,
               COLLECT { MATCH (s)-[:HAS_MEDIA]->(m)
                         RETURN m {type: m.media_type, .url, .title, .creator, .license,
                                   source: m.source_name, .source_url} } AS media,
               COLLECT { MATCH (s)-[f:FOUND_IN]->(c)
                         RETURN {country: c.key, establishment_means: f.establishment_means,
                                 occurrence_status: f.occurrence_status,
                                 conservation_status: f.conservation_status,
                                 conservation_code: f.conservation_code} } AS distribution,
               COLLECT { MATCH (s)-[:FOUND_IN]->()-[:PART_OF]->(k) RETURN DISTINCT k.name }
                   AS continents
    """, key=key)
    if not rows:
        return None
    row = rows[0]
    row["lineage"] = lineage(key)
    row["relatives"] = relatives(key)
    return row


def lineage(key):
    """Biota -> ... -> el taxón, subiendo por su único padre."""
    rows = _run("""
        MATCH p = (:Taxon {taxonID: $key})<-[:HAS_CHILD*0..]-(:Biota)
        RETURN [x IN reverse(nodes(p)) | {rank: x.rank, name: x.name, key: x.taxonID}] AS lineage
    """, key=key)
    return rows[0]["lineage"] if rows else []


def relatives(key, limit=40):
    """Padre y abuelo de una especie, con sus hermanos (para el grafo de la ficha).

    Los nombres family/genus vienen de v1: casi siempre lo son, pero cuando el
    backbone salta un rango son el abuelo y el padre reales, con su `rank`.
    """
    rows = _run("""
        MATCH (p:Taxon)-[:HAS_CHILD]->(s:Taxon {taxonID: $key})
        OPTIONAL MATCH (gp:Taxon)-[:HAS_CHILD]->(p)
        RETURN CASE WHEN gp IS NULL THEN null
                    ELSE {name: gp.name, key: gp.taxonID, rank: gp.rank} END AS family,
               {name: p.name, key: p.taxonID, rank: p.rank} AS genus,
               COLLECT { MATCH (gp)-[:HAS_CHILD]->(x) WHERE x <> p
                         RETURN {name: x.name, key: x.taxonID, rank: x.rank}
                         ORDER BY x.name LIMIT $lim } AS sibling_genera,
               COLLECT { MATCH (p)-[:HAS_CHILD]->(x) WHERE x <> s
                         RETURN {name: x.name, key: x.taxonID, rank: x.rank}
                         ORDER BY x.name LIMIT $lim } AS sibling_species
    """, key=key, lim=limit)
    return rows[0] if rows else {}


# ── Taxón superior ──

def _taxon(key):
    """Datos propios de un taxón que no es especie, o None."""
    rows = _run("""
        MATCH (n:Taxon {taxonID: $key}) WHERE NOT n:Species
        RETURN n.taxonID AS key, n.name AS name, n.rank AS rank, n.kingdom AS kingdom,
               n.speciesCount AS species_count, n.withImage AS with_image,
               n.withSound AS with_sound, n.withText AS with_text,
               n.withCommonName AS with_common_name,
               coalesce(n.countries, []) AS countries,
               coalesce(n.iucnCodes, []) AS iucn_codes, coalesce(n.iucnCounts, []) AS iucn_counts,
               COUNT { (n)-[:HAS_CHILD]->() } AS children_total
    """, key=key)
    return rows[0] if rows else None


def taxon_node(key, offset=0, limit=500):
    """Un taxón con sus hijos directos (paginados), su linaje y sus países."""
    node = _taxon(key)
    if node is None:
        return None
    # Primero los rangos más altos: Animalia tiene ~3.5k hijos directos (géneros y familias
    # sin filo) y por nombre los filos quedaban fuera de la primera página.
    rows = _run(f"""
        MATCH (n:Taxon {{taxonID: $key}})-[:HAS_CHILD]->(c)
        WITH c ORDER BY [i IN range(0, size($ranks) - 1) WHERE $ranks[i] = c.rank][0], c.name
        SKIP $offset LIMIT $limit
        RETURN CASE WHEN c:Species THEN c {{
                   .name, key: c.taxonID, .rank, conservation: c.conservation_overall_code,
                   images: COLLECT {{ MATCH (c)-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'image'
                                     RETURN m.url LIMIT 5 }},
                   common_names: c.commonNames, langs: c.commonNamesLanguages,
                   countries: COLLECT {{ MATCH (c)-[:FOUND_IN]->(co) RETURN co.key }},
                   flags: {_f(SPECIES_FLAGS, 'c')}
               }} ELSE c {{ .name, key: c.taxonID, .rank, flags: {_f(TAXON_FLAGS, 'c')} }}
               END AS child
    """, key=key, offset=offset, limit=limit, ranks=list(RANKS))
    children = [r["child"] for r in rows]
    for c in children:
        if "images" in c:
            c["image"] = c["images"][0] if c["images"] else None
            c["flags"]["countries"] = len(c["countries"])
            c["common_names"] = _by_language(c["common_names"], c.pop("langs"))
    ranks = _run("""
        MATCH (:Taxon {taxonID: $key})-[:HAS_CHILD]->(c)
        RETURN c.rank AS rank, count(*) AS n
    """, key=key)
    ranks.sort(key=lambda r: RANKS.index(r["rank"]) if r["rank"] in RANKS else len(RANKS))
    return {
        "key": key, "name": node["name"], "rank": node["rank"], "kingdom": node["kingdom"],
        "child_rank": max(ranks, key=lambda r: r["n"])["rank"] if ranks else None,
        "child_ranks": ranks,  # [{rank, n}] de más alto a más bajo, en el mismo orden que children
        "children": children,
        "children_total": node["children_total"],
        "offset": offset,
        "lineage": lineage(key),
        "countries": node["countries"],
        "species_count": node["species_count"],
        # Mismas llaves que TAXON_FLAGS de un hijo: especies descendientes con cada contenido.
        "flags": {"species": node["species_count"], "images": node["with_image"],
                  "sounds": node["with_sound"], "descriptions": node["with_text"],
                  "common_names": node["with_common_name"]},
    }


@cached
def taxon_gallery(key, offset=0, limit=250):
    """Especies con imagen a cualquier profundidad, en orden alfabético.

    Solo baja por ramas con alguna especie con imagen (withImage > 0).
    """
    node = _taxon(key)
    if node is None:
        return None
    rows = _run(f"""
        MATCH (n:Taxon {{taxonID: $key}}) ((a)-[:HAS_CHILD]->(b) WHERE b.withImage > 0)+ (s:Species)
        WITH s ORDER BY s.name SKIP $offset LIMIT $limit
        WITH s, COLLECT {{ MATCH (s)-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'image'
                          RETURN m.url LIMIT 5 }} AS images
        RETURN s {{.name, key: s.taxonID, .rank, .kingdom, common_names: s.commonNames,
                  image: images[0], images: images}} AS sp
    """, key=key, offset=offset, limit=limit)
    species = [r["sp"] for r in rows]
    return {"key": key, "rank": node["rank"], "name": node["name"], "kingdom": node["kingdom"],
            "species": species, "offset": offset, "shown": len(species),
            "total": node["with_image"]}


def _sound_card(v):
    return (f"COLLECT {{ MATCH ({v})-[:HAS_MEDIA]->(m:Media) WHERE m.media_type = 'sound' "
            f"RETURN m.url }} AS sounds, {_f(FIRST_IMAGE, v)} AS image")


@cached
def taxon_sound_tree(key, limit=250):
    """Árbol del taxón hasta sus especies con sonido (las primeras `limit`).

    Incluye los niveles intermedios reales y, para cada nodo interno, todos sus
    hijos que llevan a algún sonido (`children_pool`), para que el front pueda
    cambiar de rama sin caer en una hoja vacía.
    """
    node = _taxon(key)
    if node is None:
        return None
    paths = _run(f"""
        MATCH p = (n:Taxon {{taxonID: $key}}) ((a)-[:HAS_CHILD]->(b) WHERE b.withSound > 0)+
                  (s:Species)
        WITH s, nodes(p) AS chain ORDER BY s.name LIMIT $limit
        RETURN [x IN chain | {{rank: x.rank, name: x.name, key: x.taxonID}}] AS chain,
               s.taxonID AS leaf, s.kingdom AS kingdom, s.commonNames AS common_names,
               s.commonNamesLanguages AS langs,
               {_sound_card('s')}
    """, key=key, limit=limit)

    nodes, edges, seen_edges = {}, [], set()
    for row in paths:
        prev = None
        for x in row["chain"]:
            nid = _node_id(x["rank"], x["key"])
            nodes.setdefault(nid, {"id": nid, **x})
            if prev and (prev, nid) not in seen_edges:
                seen_edges.add((prev, nid))
                edges.append({"source": prev, "target": nid})
            prev = nid
        nodes[prev].update(kingdom=row["kingdom"], common_names=_by_language(row["common_names"], row["langs"]),
                           image=row["image"], sounds=row["sounds"])

    internal = [n["key"] for n in nodes.values() if n["rank"] != "species"]
    pool = {}
    if internal:
        rows = _run(f"""
            UNWIND $keys AS k
            MATCH (p:Taxon {{taxonID: k}})-[:HAS_CHILD]->(c) WHERE c.withSound > 0
            WITH p, c ORDER BY c.name
            RETURN p.rank AS prank, p.taxonID AS pkey, c.rank AS rank, c.taxonID AS key,
                   c.name AS name, c.kingdom AS kingdom, {_sound_card('c')}
        """, keys=internal)
        for r in rows:
            child = {"id": _node_id(r["rank"], r["key"]), "name": r["name"],
                     "rank": r["rank"], "key": r["key"]}
            if r["rank"] == "species":
                child.update(kingdom=r["kingdom"], image=r["image"], sounds=r["sounds"])
            pool.setdefault(_node_id(r["prank"], r["pkey"]), []).append(child)

    return {"key": key, "rank": node["rank"], "name": node["name"], "kingdom": node["kingdom"],
            "root_id": _node_id(node["rank"], key), "nodes": list(nodes.values()),
            "edges": edges, "children_pool": pool,
            "total_with_sound": node["with_sound"], "shown_with_sound": len(paths)}


def taxon_infographic(key):
    """Conteos, conservación, reinos, países y 3 especies destacadas al azar."""
    node = _taxon(key)
    if node is None:
        return None
    if node["kingdom"] is None:  # Biota o un dominio: abarca varios reinos
        by_kingdom = _run("""
            MATCH (:Taxon {taxonID: $key})-[:HAS_CHILD*1..2]->(k:Kingdom)
            WHERE k.speciesCount > 0
            RETURN k.name AS kingdom, k.speciesCount AS count ORDER BY count DESC
        """, key=key)
    else:
        by_kingdom = [{"kingdom": node["kingdom"], "count": node["species_count"]}]
    conservation = sorted(
        ({"code": c, "count": n} for c, n in zip(node["iucn_codes"], node["iucn_counts"])),
        key=lambda x: -x["count"])
    featured = species_cards(sample_species([key] * 3, "withContent", distinct_branches=True))
    return {"key": key, "rank": node["rank"], "name": node["name"], "kingdom": node["kingdom"],
            "species_count": node["species_count"], "images_count": node["with_image"],
            "sounds_count": node["with_sound"], "descriptions_count": node["with_text"],
            "countries": node["countries"], "conservation": conservation,
            "by_kingdom": by_kingdom, "featured": featured}


# ── Muestreo al azar ──

WEIGHTS = ("withContent", "withText", "withImage", "withSound")


def _pick(weighted, k):
    """k elementos distintos de [(item, peso)], cada uno con probabilidad proporcional a su peso."""
    pool, picked = list(weighted), []
    while pool and len(picked) < k:
        i = random.choices(range(len(pool)), weights=[w for _, w in pool])[0]
        picked.append(pool.pop(i)[0])
    return picked


def sample_species(roots, weight, distinct_branches=False):
    """Una especie al azar bajo cada raíz, uniforme entre las que tienen `weight` > 0.

    Baja por el árbol eligiendo cada hijo con probabilidad proporcional a su
    conteo precalculado: como el conteo de un padre es la suma de los de sus
    hijos, la especie final sale uniforme sin recorrer el subárbol. Es un viaje
    de ida a Neo4j por nivel, no por especie.
    distinct_branches: si todas las raíces son la misma, el primer paso elige
    hijos distintos (filos distintos bajo un reino, por ejemplo).
    """
    assert weight in WEIGHTS
    current = list(roots)
    done = [False] * len(current)
    first = True
    for _ in range(len(RANKS) + 1):
        pending = sorted({k for k, d in zip(current, done) if k is not None and not d})
        if not pending:
            break
        rows = _run(f"""
            UNWIND $keys AS k
            MATCH (p:Taxon {{taxonID: k}})
            RETURN k, p:Species AS leaf,
                   COLLECT {{ MATCH (p)-[:HAS_CHILD]->(c) WHERE c.{weight} > 0
                             RETURN [c.taxonID, c.{weight}] }} AS kids
        """, keys=pending)
        info = {r["k"]: r for r in rows}
        spread = {}
        if first and distinct_branches and len(set(current)) == 1 and current[0] in info:
            kids = info[current[0]]["kids"]
            spread = {"picks": _pick(kids, len(current))}
        for i, k in enumerate(current):
            if k is None or done[i]:
                continue
            r = info.get(k)
            if r is None:
                current[i] = None
            elif r["leaf"]:
                done[i] = True
            elif spread.get("picks"):
                current[i] = spread["picks"].pop(0)
            else:
                current[i] = (_pick(r["kids"], 1) or [None])[0]
        first = False
    return list(dict.fromkeys(k for k, d in zip(current, done) if d))


def species_cards(keys):
    """Tarjeta corta de cada especie, en el orden dado."""
    if not keys:
        return []
    rows = _run(f"""
        UNWIND $keys AS k
        MATCH (s:Taxon:Species {{taxonID: k}})
        RETURN s.taxonID AS key, s.name AS name, s.kingdom AS kingdom,
               coalesce(s.commonNames, []) AS common_names, s.commonNamesLanguages AS langs,
               s.conservation_overall_code AS conservation,
               {_f(FIRST_IMAGE, 's')} AS image, {_f(SPECIES_FLAGS, 's')} AS flags
    """, keys=keys)
    for r in rows:
        r["common_names"] = _by_language(r["common_names"], r.pop("langs"))
    return rows


def random_descendants(key, n=9):
    """Especies con descripción al azar bajo un taxón, de ramas distintas."""
    return species_cards(sample_species([key] * n, "withText", distinct_branches=True))


def random_from_kingdoms(kingdoms, n=8):
    """n tiradas: cada una elige un reino de `kingdoms` (vacío = todos) y una especie con
    descripción dentro. Un reino puede salir varias veces o ninguna."""
    rows = _run("""
        MATCH (k:Kingdom) WHERE k.withText > 0 AND (size($names) = 0 OR k.name IN $names)
        RETURN k.taxonID AS key
    """, names=kingdoms)
    if not rows:
        return []
    roots = [random.choice(rows)["key"] for _ in range(n)]
    cards = species_cards(sample_species(roots, "withText"))
    random.shuffle(cards)
    return cards


# ── Grafo de exploración ──

def kingdoms():
    return _run("MATCH (k:Kingdom) RETURN k.taxonID AS key, k.name AS name ORDER BY name")


def graph_focus(key, children=60, grandchildren=15):
    """El taxón, sus hijos con más especies y los nietos con más especies de cada hijo.

    El tope por hijo (en vez de uno global) evita que un hijo enorme deje en
    cero a sus hermanos.
    """
    rows = _run("""
        MATCH (n:Taxon {taxonID: $key})
        RETURN n {key: n.taxonID, .name, .rank, .kingdom} AS center,
               COLLECT {
                   MATCH (n)-[:HAS_CHILD]->(c)
                   WITH c ORDER BY c.speciesCount DESC, c.name LIMIT $children
                   RETURN c {key: c.taxonID, .name, .rank, .kingdom,
                             kids: COLLECT { MATCH (c)-[:HAS_CHILD]->(g)
                                             WITH g ORDER BY g.speciesCount DESC, g.name
                                             LIMIT $grandchildren
                                             RETURN g {key: g.taxonID, .name, .rank, .kingdom} }}
               } AS children
    """, key=key, children=children, grandchildren=grandchildren)
    if not rows:
        return None
    center = rows[0]["center"]
    cid = _node_id(center["rank"], center["key"])
    nodes, links = [{"id": cid, **center}], []
    for c in rows[0]["children"]:
        kids = c.pop("kids")
        nid = _node_id(c["rank"], c["key"])
        nodes.append({"id": nid, **c})
        links.append({"source": cid, "target": nid})
        for g in kids:
            gid = _node_id(g["rank"], g["key"])
            nodes.append({"id": gid, **g})
            links.append({"source": nid, "target": gid})
    return {"nodes": nodes, "links": links, "center": cid, "kingdom": center["kingdom"]}


# ── Geografía y totales ──

def country_node(code, offset=0, limit=500, kingdom=None):
    """Especies presentes en un país (paginadas), con su desglose por reino.

    kingdom filtra en la consulta: filtrar en el front solo vería la primera página.
    """
    code = code.strip().upper()
    rows = _run("""
        MATCH (co:Country {key: $code})
        OPTIONAL MATCH (co)-[:PART_OF]->(cont)
        RETURN co.name AS name, cont.name AS continent,
               COLLECT { MATCH (s:Species)-[:FOUND_IN]->(co)
                         WITH s.kingdom AS kingdom, count(*) AS n ORDER BY n DESC
                         RETURN {kingdom: kingdom, n: n} } AS by_kingdom
    """, code=code)
    if not rows:
        return None
    species = _run(f"""
        MATCH (s:Species)-[:FOUND_IN]->(:Country {{key: $code}})
        WHERE $kingdom IS NULL OR s.kingdom = $kingdom
        WITH s ORDER BY s.name SKIP $offset LIMIT $limit
        RETURN s {{.name, key: s.taxonID, .rank, .kingdom,
                  conservation: s.conservation_overall_code, common_names: s.commonNames,
                  langs: s.commonNamesLanguages,
                  image: {_f(FIRST_IMAGE, 's')}, flags: {_f(SPECIES_FLAGS, 's')}}} AS sp
    """, code=code, offset=offset, limit=limit, kingdom=kingdom)
    for r in species:
        r["sp"]["common_names"] = _by_language(r["sp"]["common_names"], r["sp"].pop("langs"))
    row = rows[0]
    return {"key": code, "name": row["name"], "rank": "country", "continent": row["continent"],
            "species_count": sum(k["n"] for k in row["by_kingdom"]),
            "by_kingdom": row["by_kingdom"], "children": [r["sp"] for r in species],
            "child_rank": "species", "offset": offset, "kingdom": kingdom,
            "children_total": sum(k["n"] for k in row["by_kingdom"] if kingdom in (None, k["kingdom"]))}


def stats():
    rows = _run("""
        MATCH (b:Biota)
        RETURN b.speciesCount AS total_species, b.withText AS with_description,
               b.withImage AS with_image, b.withSound AS with_sound
    """)
    return rows[0] if rows else {}


def data_version():
    """Fecha de creación del store: cambia con cada `neo4j-admin import`."""
    return str(run_query("CALL db.info() YIELD creationDate RETURN creationDate")[0]["creationDate"])
