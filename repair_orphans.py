"""
LinNeo -- Reparacion GENERAL de nodos huerfanos (todos los reinos y rangos).

Reconecta cualquier nodo taxonomico suelto (sin padre HAS_CHILD) a su padre
real segun el backbone de GBIF, SUBIENDO la cadena parentNameUsageID y saltando
los rangos intermedios que el modelo del grafo no tiene (subfamily, tribe,
subgenus, etc.) hasta el primer ancestro de rango principal.

PATRON DE KEYS confirmado:
  - family, genus, species  -> key == taxonID de GBIF.
  - kingdom, phylum, class, order -> numeracion propia (NO GBIF).
Por eso:
  - Al HIJO se le identifica por su key del grafo (graphkey), exacta dentro del grafo.
  - Al PADRE efectivo se le busca por key (si es family/genus, donde key==GBIF) o
    por NOMBRE (si es kingdom/phylum/class/order), con salvaguarda de unicidad.

ENTRADA:
  --orphans : CSV del grafo con columnas graphkey,name,rank
  --taxon   : Taxon.tsv del backbone

SALIDA en --out (default biodiversity_data/orphan_csv):
  - orphan_links.csv      : child_graphkey,child_name,child_rank,
                            parent_gbifid,parent_name,parent_rank
  - repair_orphans.cypher : Cypher generado a medida, idempotente
  - orphans_unresolved.csv: huerfanos que no se pudieron resolver (con motivo)

Nota de memoria: carga en RAM un mapa ligero del backbone EXCLUYENDO especies
(solo rangos kingdom..genus, ~1-2M nodos). Las especies solo se miran para los
pocos huerfanos de ese rango.

Uso:
  python repair_orphans.py --orphans orphans.csv --taxon biodiversity_data/backbone_extract/Taxon.tsv
"""
import csv, os, sys, argparse
from collections import defaultdict

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

RANK_META = {
    'kingdom': ('Kingdom', 'kingdom_key'),
    'phylum':  ('Phylum',  'phylum_key'),
    'class':   ('Class',   'class_key'),
    'order':   ('Order',   'order_key'),
    'family':  ('Family',  'family_key'),
    'genus':   ('Genus',   'genus_key'),
    'species': ('Species', 'species_key'),
}
ID_RANKS = {'family', 'genus', 'species'}        # key del grafo == taxonID GBIF
PRINCIPAL = {'kingdom', 'phylum', 'class', 'order', 'family', 'genus'}  # posibles padres
CHILD_NAME_RANKS = {'order', 'class', 'phylum'}  # huerfanos con key propia (resolver por nombre)
RANK_ORDER = ['class', 'order', 'family', 'genus', 'species']

def col_idx(header):
    want = ('taxonID', 'parentNameUsageID', 'taxonRank', 'taxonomicStatus',
            'canonicalName', 'scientificName')
    idx = {n: header.index(n) for n in want if n in header}
    need = ['taxonID', 'parentNameUsageID', 'taxonRank']
    missing = [c for c in need if c not in idx]
    if missing:
        print(f"ERROR: faltan columnas en Taxon.tsv: {missing}")
        sys.exit(1)
    return idx

def get(parts, idx, key):
    i = idx.get(key)
    if i is None or i >= len(parts):
        return ''
    return (parts[i] or '').strip()

def to_int(s):
    try:
        return int(s)
    except (ValueError, TypeError):
        return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--orphans', required=True)
    ap.add_argument('--taxon', required=True)
    ap.add_argument('--out', default='biodiversity_data/orphan_csv')
    args = ap.parse_args()

    for p in (args.orphans, args.taxon):
        if not os.path.exists(p):
            print(f"ERROR: no existe {p}")
            sys.exit(1)
    os.makedirs(args.out, exist_ok=True)

    # ---- Cargar huerfanos del grafo ----
    orphan_id = {}        # gbifid(int) -> (graphkey, name, rank) para family/genus/species
    orphan_name = {}      # (name, rank) -> graphkey                para order/class/phylum
    name_keys = set()
    orphan_names_set = set()   # (name,rank) de TODOS los huerfanos (para fallback por nombre)
    species_gbifids = set()
    n_orphans = 0
    discarded = 0
    with open(args.orphans, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            gk = (row.get('graphkey') or '').strip()
            name = (row.get('name') or '').strip()
            rank = (row.get('rank') or '').strip().lower()
            if rank not in RANK_META:
                discarded += 1
                continue
            n_orphans += 1
            if name:
                orphan_names_set.add((name, rank))
            if rank in ID_RANKS:
                gbid = to_int(gk)
                if gbid is None:
                    discarded += 1
                    continue
                orphan_id[gbid] = (gk, name, rank)
                if rank == 'species':
                    species_gbifids.add(gbid)
            else:  # order/class/phylum: key propia
                orphan_name[(name, rank)] = gk
                name_keys.add((name, rank))
    print(f"Huerfanos leidos: {n_orphans:,} "
          f"(por id: {len(orphan_id):,} | por nombre: {len(orphan_name):,} "
          f"| descartados: {discarded:,})")

    # ---- UNA pasada del TSV: mapa ligero (sin especies) ----
    with open(args.taxon, encoding='utf-8') as f:
        header = f.readline().rstrip('\r\n').split('\t')
    idx = col_idx(header)

    tid_parent = {}                  # tid -> parentID
    tid_rank = {}                    # tid -> rank
    tid_name = {}                    # tid -> nombre (rangos no-species)
    orphan_species_parent = {}       # species huerfana -> parentID
    name_to_tids = defaultdict(set)  # (name,rank) -> {tid}  para order/class huerfanos
    n_read = 0
    with open(args.taxon, encoding='utf-8') as f:
        rd = csv.reader(f, delimiter='\t')
        next(rd, None)
        for parts in rd:
            n_read += 1
            if n_read % 1000000 == 0:
                print(f"  ...{n_read:,}")
            tid = to_int(get(parts, idx, 'taxonID'))
            if tid is None:
                continue
            rank = get(parts, idx, 'taxonRank').lower()
            pid = to_int(get(parts, idx, 'parentNameUsageID')) or 0
            if rank == 'species':
                if tid in species_gbifids:
                    orphan_species_parent[tid] = pid
                continue  # no guardar el grueso de especies
            tid_parent[tid] = pid
            tid_rank[tid] = sys.intern(rank) if rank else ''
            cname = get(parts, idx, 'canonicalName')
            sname = get(parts, idx, 'scientificName')
            tid_name[tid] = cname or sname
            if orphan_names_set:
                if (cname, rank) in orphan_names_set:
                    name_to_tids[(cname, rank)].add(tid)
                elif (sname, rank) in orphan_names_set:
                    name_to_tids[(sname, rank)].add(tid)

    # ---- Subir la cadena hasta el primer ancestro de rango principal ----
    def effective_parent(start_pid):
        cur = start_pid
        seen = set()
        while cur and cur in tid_rank and cur not in seen:
            seen.add(cur)
            if tid_rank[cur] in PRINCIPAL:
                return cur
            cur = tid_parent.get(cur, 0)
        return None

    links = []
    unresolved = []
    rescued_by_name = 0

    def emit(gk, cname, crank, start_pid):
        if not start_pid:
            unresolved.append((gk, cname, crank, '', 'sin parentNameUsageID'))
            return
        pe = effective_parent(start_pid)
        if not pe:
            unresolved.append((gk, cname, crank, str(start_pid),
                               'sin ancestro de rango principal'))
            return
        pname = tid_name.get(pe, '')
        prank = tid_rank.get(pe, '')
        if prank not in RANK_META or not pname:
            unresolved.append((gk, cname, crank, str(pe), 'padre sin nombre/rango'))
            return
        links.append((gk, cname, crank, pe, pname, prank))

    def start_by_name(name, rank):
        """Fallback: localizar el taxon por nombre+rango en el TSV (unico) y
        devolver su parentID. Devuelve (start, motivo_error)."""
        tids = name_to_tids.get((name, rank), set())
        if len(tids) == 1:
            return tid_parent.get(next(iter(tids))), None
        if len(tids) > 1:
            return None, 'nombre ambiguo en TSV'
        return None, None  # no hallado por nombre

    # family/genus/species huerfanos (por id; con fallback por nombre)
    for gbid, (gk, name, rank) in orphan_id.items():
        if rank == 'species':
            start = orphan_species_parent.get(gbid)
            if start is None:
                unresolved.append((gk, name, rank, '', 'species no hallada en TSV'))
                continue
        elif gbid in tid_parent:
            start = tid_parent[gbid]
        else:
            # la key del grafo no esta en el backbone -> intentar por nombre
            start, err = start_by_name(name, rank)
            if start is None:
                if err:
                    unresolved.append((gk, name, rank, '', err))
                else:
                    unresolved.append((gk, name, rank, '',
                                       f'{rank} no hallado (ni por id ni por nombre)'))
                continue
            rescued_by_name += 1
        emit(gk, name, rank, start)

    # order/class/phylum huerfanos (por nombre)
    for k, gk in orphan_name.items():
        name, rank = k
        start, err = start_by_name(name, rank)
        if start is None and err is None and k in name_keys:
            unresolved.append((gk, name, rank, '', 'nombre no hallado en TSV'))
        elif err:
            unresolved.append((gk, name, rank, '', err))
        else:
            emit(gk, name, rank, start)

    # ---- Escribir orphan_links.csv ----
    links_path = os.path.join(args.out, 'orphan_links.csv')
    with open(links_path, 'w', newline='', encoding='utf-8') as out:
        w = csv.writer(out)
        w.writerow(['child_graphkey', 'child_name', 'child_rank',
                    'parent_gbifid', 'parent_name', 'parent_rank'])
        for row in links:
            w.writerow(row)
    print(f"escrito {links_path} ({len(links):,} enlaces)")

    if unresolved:
        unp = os.path.join(args.out, 'orphans_unresolved.csv')
        with open(unp, 'w', newline='', encoding='utf-8') as out:
            w = csv.writer(out)
            w.writerow(['graphkey', 'name', 'rank', 'ref_id', 'motivo'])
            for row in unresolved:
                w.writerow(row)
        print(f"AVISO: {len(unresolved):,} sin resolver -> {unp}")
        mot = defaultdict(int)
        for row in unresolved:
            mot[row[4]] += 1
        for m, n in sorted(mot.items(), key=lambda x: -x[1]):
            print(f"    {n:,}  {m}")
    if rescued_by_name:
        print(f"Rescatados por nombre (key no estaba en el TSV): {rescued_by_name:,}")

    # ---- Generar repair_orphans.cypher ----
    combos = sorted({(c[2], c[5]) for c in links},
                    key=lambda cp: (RANK_ORDER.index(cp[0]) if cp[0] in RANK_ORDER else 99,
                                    cp[1]))
    cypher_path = os.path.join(args.out, 'repair_orphans.cypher')
    with open(cypher_path, 'w', encoding='utf-8') as out:
        w = out.write
        w("// LinNeo -- Reparacion GENERAL de huerfanos (generado por repair_orphans.py).\n")
        w("// Requisito: copiar orphan_links.csv a la carpeta import/ de Neo4j.\n")
        w("// Idempotente: MERGE + restriccion de orfandad. No toca lo ya conectado.\n\n")
        w("// Indices para que los MATCH sean rapidos.\n")
        for rk in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species'):
            lbl, kp = RANK_META[rk]
            w(f"CREATE INDEX {kp} IF NOT EXISTS FOR (n:{lbl}) ON (n.{kp});\n")
        for rk in ('kingdom', 'phylum', 'class', 'order'):
            lbl, _ = RANK_META[rk]
            w(f"CREATE INDEX {lbl.lower()}_name IF NOT EXISTS FOR (n:{lbl}) ON (n.name);\n")
        w("\n// Asegurar el Kingdom Fungi (key 9) y kingdom_key de sus especies.\n")
        w("MERGE (k:Kingdom {name:'Fungi'}) ON CREATE SET k.kingdom_key = 9;\n")
        w("MATCH (s:Species {kingdom:'Fungi'}) WHERE s.kingdom_key IS NULL "
          "SET s.kingdom_key = 9;\n\n")
        for crank, prank in combos:
            clbl, ckp = RANK_META[crank]
            plbl, pkp = RANK_META[prank]
            w(f"// hijo={crank}  ->  padre={prank}\n")
            w("LOAD CSV WITH HEADERS FROM 'file:///orphan_links.csv' AS row\n")
            w(f"WITH row WHERE row.child_rank = '{crank}' AND row.parent_rank = '{prank}'\n")
            w(f"MATCH (c:{clbl} {{{ckp}: toInteger(row.child_graphkey)}})\n")
            w("WHERE NOT ()-[:HAS_CHILD]->(c)\n")
            if prank in ID_RANKS:
                w(f"MATCH (p:{plbl} {{{pkp}: toInteger(row.parent_gbifid)}})\n")
                w("MERGE (p)-[:HAS_CHILD]->(c);\n\n")
            else:
                w(f"MATCH (p:{plbl} {{name: row.parent_name}})\n")
                w("WITH c, collect(DISTINCT p) AS ps WHERE size(ps) = 1\n")
                w("WITH c, ps[0] AS p\n")
                w("MERGE (p)-[:HAS_CHILD]->(c);\n\n")
        w("// Verificacion: huerfanos restantes por rango.\n")
        w("MATCH (n) WHERE (n:Phylum OR n:Class OR n:Order OR n:Family OR n:Genus "
          "OR n:Species) AND NOT ()-[:HAS_CHILD]->(n)\n")
        w("RETURN labels(n)[0] AS rango, count(*) AS huerfanos ORDER BY huerfanos DESC;\n")
    print(f"escrito {cypher_path} ({len(combos)} bloques de reconexion)")

    by = defaultdict(int)
    for c in links:
        by[(c[2], c[5])] += 1
    print("\nResumen de enlaces (hijo -> padre):")
    for (cr, pr), n in sorted(by.items(), key=lambda x: -x[1]):
        print(f"  {cr:8s} -> {pr:8s}: {n:,}")

if __name__ == '__main__':
    main()