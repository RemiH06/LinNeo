"""
LinNeo -- Diagnostico de los unresolved de dedupe_parents.py.

Cruza dedupe_unresolved.csv con dupe_parents.csv (para recuperar los padres
reales de cada nodo) y con Taxon.tsv (para ubicar cada padre en el backbone:
su rango ancestro mas alto encontrado, y si dos padres comparten texto muy
similar -- posible typo/duplicado no detectado por el criterio de arbol).

No decide nada por si solo: solo imprime una tabla legible para que el
usuario clasifique a ojo los casos, antes de tocar el grafo.

Salida: dedupe_unresolved_detalle.csv en el mismo --out.

Uso:
  python diagnostico_unresolved.py --unresolved biodiversity_data/orphan_csv/dedupe_unresolved.csv \
                                    --dupes biodiversity_data/orphan_csv/dupe_parents.csv \
                                    --taxon biodiversity_data/backbone_extract/Taxon.tsv
"""
import csv, os, sys, argparse
from collections import defaultdict
from difflib import SequenceMatcher

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

RANK_ORDER = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
FUSIBLE_FROM = {'order', 'class', 'phylum', 'kingdom'}


def col_idx(header):
    want = ('taxonID', 'parentNameUsageID', 'taxonRank', 'taxonomicStatus',
            'canonicalName', 'scientificName', 'kingdom')
    return {n: header.index(n) for n in want if n in header}


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


def name_sim(a, b):
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--unresolved', required=True)
    ap.add_argument('--dupes', required=True)
    ap.add_argument('--taxon', required=True)
    ap.add_argument('--out', default='biodiversity_data/orphan_csv')
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # ---- nodos unresolved ----
    unresolved_keys = set()
    with open(args.unresolved, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            unresolved_keys.add((row['child_key'].strip(), row['child_rank'].strip().lower()))
    print(f"Nodos unresolved a diagnosticar: {len(unresolved_keys):,}")

    # ---- sus padres reales, desde dupe_parents.csv ----
    parents_of = defaultdict(list)  # (ck,cr) -> [(pk,pn,pr)]
    with open(args.dupes, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            ck = row['child_key'].strip()
            cr = row['child_rank'].strip().lower()
            key = (ck, cr)
            if key in unresolved_keys:
                parents_of[key].append((row['parent_key'].strip(),
                                         row['parent_name'].strip(),
                                         row['parent_rank'].strip().lower()))

    # nombres de padres que necesitamos ubicar en el backbone
    parent_names = {(pn, pr) for plist in parents_of.values() for (_, pn, pr) in plist}

    # ---- pasada por Taxon.tsv ----
    with open(args.taxon, encoding='utf-8') as f:
        header = f.readline().rstrip('\r\n').split('\t')
    idx = col_idx(header)
    has_status = 'taxonomicStatus' in idx

    tid_parent, tid_name, tid_rank = {}, {}, {}
    name_to_tids = defaultdict(list)
    n_read = 0
    with open(args.taxon, encoding='utf-8') as f:
        rd = csv.reader(f, delimiter='\t')
        next(rd, None)
        for parts in rd:
            n_read += 1
            tid = to_int(get(parts, idx, 'taxonID'))
            if tid is None:
                continue
            rank = get(parts, idx, 'taxonRank').lower()
            status = get(parts, idx, 'taxonomicStatus').upper() if has_status else 'ACCEPTED'
            pid = to_int(get(parts, idx, 'parentNameUsageID')) or 0
            cname = get(parts, idx, 'canonicalName')
            sname = get(parts, idx, 'scientificName')
            nm = cname or sname
            tid_parent[tid] = pid
            tid_name[tid] = nm
            tid_rank[tid] = rank
            if status != 'ACCEPTED' or rank == 'species':
                continue
            if (cname, rank) in parent_names:
                name_to_tids[(cname, rank)].append(tid)
            elif (sname, rank) in parent_names:
                name_to_tids[(sname, rank)].append(tid)

    def ancestor_chain(tid):
        out = []
        cur = tid_parent.get(tid, 0)
        seen = set()
        while cur and cur in tid_parent and cur not in seen:
            seen.add(cur)
            out.append((cur, tid_name.get(cur), tid_rank.get(cur)))
            cur = tid_parent.get(cur, 0)
        return out

    def root_at_or_above(tid, ranks):
        for (atid, nm, ar) in ancestor_chain(tid):
            if ar in ranks:
                return (atid, nm, ar)
        return (None, None, None)

    # ---- armar detalle por nodo ----
    out_rows = []
    for (ck, cr), plist in sorted(parents_of.items()):
        names = ', '.join(f"{pn}({pr})" for (_, pn, pr) in plist)
        roots = []
        for (pk, pn, pr) in plist:
            tids = name_to_tids.get((pn, pr), [])
            if not tids:
                roots.append(f"{pn}=NO_EN_BACKBONE")
                continue
            atid, anm, ar = root_at_or_above(tids[0], FUSIBLE_FROM)
            if atid is None:
                roots.append(f"{pn}=SIN_ANCESTRO_ORDER+")
            else:
                roots.append(f"{pn}->raiz:{anm}({ar})")
        roots_str = ' | '.join(roots)

        # similitud de nombre entre cada par de padres (posible typo/duplicado)
        sims = []
        for i in range(len(plist)):
            for j in range(i + 1, len(plist)):
                s = name_sim(plist[i][1], plist[j][1])
                if s >= 0.80:
                    sims.append(f"{plist[i][1]}~{plist[j][1]}={s:.2f}")
        sims_str = '; '.join(sims) if sims else ''

        out_rows.append([ck, cr, names, roots_str, sims_str])

    out_path = os.path.join(args.out, 'dedupe_unresolved_detalle.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as out:
        w = csv.writer(out)
        w.writerow(['child_key', 'child_rank', 'padres', 'raiz_por_padre', 'similitud_alta'])
        for row in out_rows:
            w.writerow(row)
    print(f"escrito {out_path} ({len(out_rows):,} filas)")

    # ---- resumen rapido en consola ----
    n_typo = sum(1 for r in out_rows if r[4])
    n_no_backbone = sum(1 for r in out_rows if 'NO_EN_BACKBONE' in r[3])
    print(f"\nCon posible typo/duplicado (similitud>=0.80 entre padres): {n_typo:,}")
    print(f"Con al menos un padre no encontrado en el backbone: {n_no_backbone:,}")


if __name__ == '__main__':
    main()