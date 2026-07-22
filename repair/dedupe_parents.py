"""
LinNeo -- Deduplicacion de padres (v4, basada en aristas reales + fusion +
reino real + unico verificable).

Entrada: un CSV con UNA FILA POR ARISTA padre de cada nodo con >1 padre:
  child_key,child_name,child_rank,child_kingdom,parent_key,parent_name,parent_rank

Resolucion del padre canonico de cada hijo, en orden:
  1) KEY DIRECTA: si child_key es un taxonID ACEPTADO en el backbone, su
     parentNameUsageID real decide sin ambiguedad (no se indexa por nombre).
  2) NOMBRE+REINO+LINAJE: si el hijo no tiene key propia en el backbone, se
     busca su nombre (filtrando taxonomicStatus=ACCEPTED y kingdom=
     child_kingdom) y se sube el linaje; se conserva el padre cuyo nombre
     aparece en ese linaje.
  3) FUSION: si tras (1)/(2) quedan >=2 padres candidatos empatados PERO
     todos resuelven en el backbone a tids cuyo ancestro comun sube hasta
     Order/Class/Phylum/Kingdom (mismo arbol taxonomico), se trata como UN
     SOLO taxon duplicado en el grafo: se elige superviviente (el que tenga
     key propia en backbone si solo uno la tiene, o el de mayor key si hay
     empate) y se reapuntan los hijos del duplicado perdedor al
     superviviente, borrando el nodo perdedor.
  4) REINO REAL DEL NODO: si todos los padres se ubican en el backbone pero
     en arboles DISTINTOS (no fusionables), y se conoce el reino real del
     nodo (child_kingdom, deducido de sus especies en el grafo), se conserva
     el padre cuyo propio reino en el backbone coincide -- solo si eso deja
     EXACTAMENTE un candidato.
  5) UNICO VERIFICABLE: si de todos los padres candidatos solo UNO tiene
     entrada en el backbone (nombre+rango encontrados) y los demas no
     existen ahi en absoluto, ese unico verificable es el canonico por
     descarte (no hay candidato real con quien competir).
  6) Si nada de lo anterior aplica -> unresolved, con motivo.

Seguridad:
  - Nunca deja huerfano: el canonico (cualquiera de los 5 metodos) siempre
    se conserva o se reapunta; nunca se borra una arista sin que el hijo
    quede con al menos un padre valido.
  - El borrado/reapuntado de aristas usa SIEMPRE keys exactas del grafo,
    nunca nombre como criterio de borrado (el nombre solo decide CUAL
    conservar).
  - La fusion exige coincidencia de linaje hasta rango Order o superior;
    jamas fusiona solo por similitud de nombre ni solo por mismo reino.
  - El paso de reino real (4) solo decide si los padres llevan a reinos
    DISTINTOS y deja EXACTAMENTE un candidato del reino correcto; nunca
    elige entre dos padres del mismo reino por este metodo.
  - El paso de unico verificable (5) nunca se aplica si dos o mas padres
    tienen entrada en el backbone, aunque sea ambigua entre ellos.

SALIDA en --out (default biodiversity_data/orphan_csv):
  - dedupe_delete.csv     : child_key,child_rank,parent_key,parent_rank,parent_name
                            (aristas espurias a borrar; el padre canonico sobrevive)
  - dedupe_merge.csv      : loser_key,loser_rank,loser_name,survivor_key,survivor_rank,
                            survivor_name (nodos duplicados a fusionar)
  - dedupe_parents.cypher : DELETE de aristas espurias + reapuntado/borrado de fusiones
  - dedupe_unresolved.csv : nodos no tocados, con motivo

Uso:
  python dedupe_parents.py --dupes biodiversity_data/orphan_csv/dupe_parents.csv \
                           --taxon biodiversity_data/backbone_extract/Taxon.tsv
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
RANK_ORDER = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
# Rangos a partir de los cuales se acepta coincidencia de linaje para fusionar.
FUSIBLE_FROM = {'order', 'class', 'phylum', 'kingdom'}


def col_idx(header):
    want = ('taxonID', 'parentNameUsageID', 'taxonRank', 'taxonomicStatus',
            'canonicalName', 'scientificName', 'kingdom')
    idx = {n: header.index(n) for n in want if n in header}
    need = ['taxonID', 'parentNameUsageID', 'taxonRank', 'canonicalName']
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
    ap.add_argument('--dupes', required=True)
    ap.add_argument('--taxon', required=True)
    ap.add_argument('--out', default='biodiversity_data/orphan_csv')
    args = ap.parse_args()

    for p in (args.dupes, args.taxon):
        if not os.path.exists(p):
            print(f"ERROR: no existe {p}")
            sys.exit(1)
    os.makedirs(args.out, exist_ok=True)

    # ---- Cargar aristas, agrupar por hijo ----
    children = {}      # (child_key,child_rank) -> {name, kingdom, parents:[(pkey,pname,prank)]}
    name_keys = set()  # (name,rank) de hijos, para indexar el TSV solo por lo que necesitamos
    with open(args.dupes, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            ck = (row.get('child_key') or '').strip()
            cn = (row.get('child_name') or '').strip()
            cr = (row.get('child_rank') or '').strip().lower()
            kg = (row.get('child_kingdom') or '').strip()
            pk = (row.get('parent_key') or '').strip()
            pn = (row.get('parent_name') or '').strip()
            pr = (row.get('parent_rank') or '').strip().lower()
            if cr not in RANK_META or not ck:
                continue
            key = (ck, cr)
            if key not in children:
                children[key] = {'name': cn, 'kingdom': kg, 'parents': []}
                if cn:
                    name_keys.add((cn, cr))
            children[key]['parents'].append((pk, pn, pr))
    n_children = len(children)
    n_edges = sum(len(c['parents']) for c in children.values())
    print(f"Hijos con >1 padre: {n_children:,} | aristas candidatas: {n_edges:,}")

    # set de nombres de padres candidatos, para poder indexarlos tambien
    # (necesario para chequear linaje compartido en el paso de fusion)
    parent_names = set()
    for info in children.values():
        for (_, pn, pr) in info['parents']:
            if pn:
                parent_names.add((pn, pr))

    # ---- Pasada por el TSV ----
    with open(args.taxon, encoding='utf-8') as f:
        header = f.readline().rstrip('\r\n').split('\t')
    idx = col_idx(header)
    has_status = 'taxonomicStatus' in idx

    tid_parent = {}          # tid -> parent tid
    tid_name = {}            # tid -> nombre
    tid_rank = {}            # tid -> rango (lower)
    accepted_tids = set()    # taxonID que son ACCEPTED (para resolucion por key directa)
    name_to_tids = defaultdict(list)   # (name,rank) -> [(tid, kingdom)] ; solo ACCEPTED
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
            status = get(parts, idx, 'taxonomicStatus').upper() if has_status else 'ACCEPTED'
            pid = to_int(get(parts, idx, 'parentNameUsageID')) or 0
            cname = get(parts, idx, 'canonicalName')
            sname = get(parts, idx, 'scientificName')
            nm = cname or sname
            # el linaje (subir cadena) usa TODOS los tid, aceptados o no: un
            # ancestro puede ser referenciado aunque su propio registro no se
            # use como fuente de resolucion por nombre.
            tid_parent[tid] = pid
            tid_name[tid] = nm
            tid_rank[tid] = rank
            if status != 'ACCEPTED' or rank == 'species':
                continue
            accepted_tids.add(tid)
            if (cname, rank) in name_keys or (sname, rank) in name_keys:
                key = (cname, rank) if (cname, rank) in name_keys else (sname, rank)
                name_to_tids[key].append((tid, kingdom := get(parts, idx, 'kingdom')))
            if (cname, rank) in parent_names or (sname, rank) in parent_names:
                key = (cname, rank) if (cname, rank) in parent_names else (sname, rank)
                name_to_tids[key].append((tid, get(parts, idx, 'kingdom')))

    def ancestor_chain(tid):
        """Lista de (tid, nombre, rango) de ancestros, del mas cercano al mas lejano."""
        out = []
        cur = tid_parent.get(tid, 0)
        seen = set()
        while cur and cur in tid_parent and cur not in seen:
            seen.add(cur)
            out.append((cur, tid_name.get(cur), tid_rank.get(cur)))
            cur = tid_parent.get(cur, 0)
        return out

    def ancestor_names(tid):
        return {nm for (_, nm, _) in ancestor_chain(tid) if nm}

    def ancestor_at_or_above(tid, ranks):
        """Primer ancestro (tid) cuyo rango esta en `ranks`."""
        for (atid, _, ar) in ancestor_chain(tid):
            if ar in ranks:
                return atid
        return None

    # ---- Resolucion ----
    deletes = []     # (child_key, child_rank, parent_key, parent_rank, parent_name)
    merges = []       # (loser_key, loser_rank, loser_name, survivor_key, survivor_rank, survivor_name)
    unresolved = []  # (child_key, child_name, child_rank, child_kingdom, motivo)
    resolved_key = 0
    resolved_lineage = 0
    resolved_merge = 0
    resolved_kingdom = 0
    resolved_only_verifiable = 0

    for (ck, cr), info in children.items():
        cn, kg = info['name'], info['kingdom']
        parents = info['parents']

        # --- Paso 1: key directa del hijo en el backbone ---
        ck_int = to_int(ck)
        resolved_here = False
        if ck_int is not None and ck_int in accepted_tids:
            parent_tid = tid_parent.get(ck_int, 0)
            parent_name_bb = tid_name.get(parent_tid)
            matches = [pn for (_, pn, _) in parents if pn == parent_name_bb]
            if parent_name_bb and matches:
                canon_name = parent_name_bb
                for (pk, pn, pr) in parents:
                    if pn != canon_name:
                        deletes.append((ck, cr, pk, pr, pn))
                resolved_key += 1
                resolved_here = True
            # si la key directa no calza con ninguno de los padres actuales,
            # no se usa a ciegas (podria ser un taxonID no relacionado); cae
            # al paso 2 igual que si no tuviera key propia.
        if resolved_here:
            continue

        # --- Paso 2: nombre + reino + linaje ---
        cands = name_to_tids.get((cn, cr), [])
        if kg:
            cands = [(t, k) for (t, k) in cands if k == kg]
        lineage_done = False
        if cands:
            anc = set()
            for tid, _ in cands:
                anc |= ancestor_names(tid)
            if anc:
                in_lineage = [(pk, pn, pr) for (pk, pn, pr) in parents if pn in anc]
                canon_names = {pn for (_, pn, _) in in_lineage}
                if len(canon_names) == 1:
                    canon_name = next(iter(canon_names))
                    for (pk, pn, pr) in parents:
                        if pn != canon_name:
                            deletes.append((ck, cr, pk, pr, pn))
                    resolved_lineage += 1
                    lineage_done = True
        if lineage_done:
            continue

        # --- Paso 3: fusion (los padres candidatos resuelven al mismo arbol) ---
        # Solo aplica si los nombres de TODOS los padres candidatos resuelven
        # en el backbone a tids cuyo ancestro comun sube hasta Order/Class/
        # Phylum/Kingdom. Si alguno no se ubica, no se fusiona.
        parent_roots = []
        ok_for_merge = True
        for (pk, pn, pr) in parents:
            ptids = name_to_tids.get((pn, pr), [])
            if not ptids:
                ok_for_merge = False
                break
            ptid = ptids[0][0]
            root = ancestor_at_or_above(ptid, FUSIBLE_FROM)
            if root is None:
                ok_for_merge = False
                break
            parent_roots.append((pk, pn, pr, ptid, root))

        if ok_for_merge and len(parent_roots) >= 2:
            roots = {r for (_, _, _, _, r) in parent_roots}
            if len(roots) == 1:
                # mismo arbol (comparten ancestro hasta Order o mas arriba):
                # es un duplicado de nodo, no ambiguedad real. Elegimos
                # superviviente: preferimos el padre cuya propia key es un
                # taxonID ACEPTADO del backbone; si ninguno o varios, el de
                # mayor key numerica (estable y determinista).
                def has_own_key(pk):
                    pk_int = to_int(pk)
                    return pk_int is not None and pk_int in accepted_tids

                with_key = [p for p in parent_roots if has_own_key(p[0])]
                pool = with_key if len(with_key) == 1 else parent_roots
                survivor = max(pool, key=lambda p: to_int(p[0]) or -1)
                sk, sn, sr = survivor[0], survivor[1], survivor[2]
                for (pk, pn, pr, _, _) in parent_roots:
                    if pk != sk:
                        merges.append((pk, pr, pn, sk, sr, sn))
                resolved_merge += 1
                continue

        # --- Paso 4: reino real del nodo desempata entre arboles distintos ---
        # Si TODOS los padres se ubicaron en el backbone (parent_roots tiene
        # un elemento por cada padre, aunque sus raices NO coincidan) y
        # conocemos el reino real del nodo (child_kingdom, deducido de sus
        # especies en el grafo), nos quedamos con el/los padres cuyo propio
        # reino en el backbone coincide con ese reino real. Solo decide si
        # eso deja EXACTAMENTE un padre; si deja 0 o >1, no se toca aqui.
        if kg and len(parent_roots) == len(parents) and len(parent_roots) >= 2:
            by_kingdom = []
            for (pk, pn, pr, ptid, _) in parent_roots:
                ptids = name_to_tids.get((pn, pr), [])
                kingdoms = {k for (t, k) in ptids if t == ptid}
                if kg in kingdoms:
                    by_kingdom.append((pk, pn, pr))
            if len(by_kingdom) == 1:
                canon_name = by_kingdom[0][1]
                for (pk, pn, pr) in parents:
                    if pn != canon_name:
                        deletes.append((ck, cr, pk, pr, pn))
                resolved_kingdom += 1
                continue

        # --- Paso 5: unico padre verificable en el backbone ---
        # Si de todos los padres candidatos solo UNO tiene entrada en el
        # backbone (nombre+rango encontrados) y los demas no existen ahi en
        # absoluto, no hay competencia real: el unico verificable es el
        # canonico por descarte.
        verifiable = [(pk, pn, pr) for (pk, pn, pr) in parents if name_to_tids.get((pn, pr))]
        if len(verifiable) == 1 and len(parents) >= 2:
            canon_name = verifiable[0][1]
            for (pk, pn, pr) in parents:
                if pn != canon_name:
                    deletes.append((ck, cr, pk, pr, pn))
            resolved_only_verifiable += 1
            continue

        unresolved.append((ck, cn, cr, kg, 'ambiguo: padres no resuelven a un unico canonico ni comparten arbol'))


    # ---- Escribir dedupe_delete.csv ----
    del_path = os.path.join(args.out, 'dedupe_delete.csv')
    with open(del_path, 'w', newline='', encoding='utf-8') as out:
        w = csv.writer(out)
        w.writerow(['child_key', 'child_rank', 'parent_key', 'parent_rank', 'parent_name'])
        for row in deletes:
            w.writerow(row)
    print(f"escrito {del_path} ({len(deletes):,} aristas a borrar; "
          f"{resolved_key:,} por key directa + {resolved_lineage:,} por linaje + "
          f"{resolved_kingdom:,} por reino real + {resolved_only_verifiable:,} por unico verificable)")

    # ---- Salvaguarda: un loser no puede aparecer en mas de un grupo de fusion ----
    # (caso limite: el mismo nodo-padre participa como duplicado en mas de un
    # hijo distinto, con supervivientes distintos resueltos). Si ocurre, se
    # descartan TODAS sus filas de merge y se reportan como unresolved, en vez
    # de arriesgar un DETACH DELETE sobre un nodo ya fusionado/borrado.
    loser_seen = defaultdict(list)
    for i, (lk, lr, ln, sk, sr, sn) in enumerate(merges):
        loser_seen[(lk, lr)].append(i)
    conflicting = {k for k, idxs in loser_seen.items() if len(idxs) > 1
                   and len({merges[i][3] for i in idxs}) > 1}
    if conflicting:
        kept = []
        for (lk, lr, ln, sk, sr, sn) in merges:
            if (lk, lr) in conflicting:
                unresolved.append((lk, ln, lr, '', 'loser de fusion con supervivientes distintos en grupos diferentes'))
            else:
                kept.append((lk, lr, ln, sk, sr, sn))
        print(f"AVISO: {len(conflicting):,} nodos perdedores con fusiones conflictivas, movidos a unresolved")
        merges = kept

    # ---- Escribir dedupe_merge.csv ----
    merge_path = os.path.join(args.out, 'dedupe_merge.csv')
    with open(merge_path, 'w', newline='', encoding='utf-8') as out:
        w = csv.writer(out)
        w.writerow(['loser_key', 'loser_rank', 'loser_name', 'survivor_key', 'survivor_rank', 'survivor_name'])
        for row in merges:
            w.writerow(row)
    print(f"escrito {merge_path} ({len(merges):,} nodos perdedores a fusionar; "
          f"{resolved_merge:,} grupos de fusion)")

    if unresolved:
        unp = os.path.join(args.out, 'dedupe_unresolved.csv')
        with open(unp, 'w', newline='', encoding='utf-8') as out:
            w = csv.writer(out)
            w.writerow(['child_key', 'child_name', 'child_rank', 'child_kingdom', 'motivo'])
            for row in unresolved:
                w.writerow(row)
        print(f"AVISO: {len(unresolved):,} hijos sin tocar -> {unp}")
        mot = defaultdict(int)
        for row in unresolved:
            mot[row[4]] += 1
        for m, n in sorted(mot.items(), key=lambda x: -x[1]):
            print(f"    {n:,}  {m}")

    # ---- Generar dedupe_parents.cypher ----
    combos = sorted({(d[1], d[3]) for d in deletes},
                    key=lambda cp: (RANK_ORDER.index(cp[0]) if cp[0] in RANK_ORDER else 99,
                                    RANK_ORDER.index(cp[1]) if cp[1] in RANK_ORDER else 99))
    merge_ranks = sorted({r for (_, r, _, _, _, _) in merges},
                         key=lambda r: RANK_ORDER.index(r) if r in RANK_ORDER else 99)
    cyp = os.path.join(args.out, 'dedupe_parents.cypher')
    with open(cyp, 'w', encoding='utf-8') as out:
        w = out.write
        w("// LinNeo -- Deduplicacion de padres v3 (generado por dedupe_parents.py).\n")
        w("// Requisito: copiar dedupe_delete.csv y dedupe_merge.csv a import/ de Neo4j.\n")
        w("// Parte 1: borra aristas HAS_CHILD espurias (padre canonico se conserva).\n")
        w("// Parte 2: fusiona nodos duplicados (mismo arbol taxonomico) reapuntando\n")
        w("//          sus hijos al superviviente y borrando el nodo perdedor.\n")
        w("// No deja huerfanos en ninguna de las dos partes. Idempotente.\n\n")
        for rk in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus'):
            lbl, kp = RANK_META[rk]
            w(f"CREATE INDEX {kp} IF NOT EXISTS FOR (n:{lbl}) ON (n.{kp});\n")
        w("\n// ---- Parte 1: aristas espurias ----\n\n")
        for crank, prank in combos:
            clbl, ckp = RANK_META[crank]
            plbl, pkp = RANK_META[prank]
            w(f"// borrar arista espuria  {prank} -> {crank}\n")
            w("LOAD CSV WITH HEADERS FROM 'file:///dedupe_delete.csv' AS row\n")
            w(f"WITH row WHERE row.child_rank = '{crank}' AND row.parent_rank = '{prank}'\n")
            w(f"MATCH (p:{plbl} {{{pkp}: toInteger(row.parent_key)}})"
              f"-[r:HAS_CHILD]->(c:{clbl} {{{ckp}: toInteger(row.child_key)}})\n")
            w("DELETE r;\n\n")

        w("// ---- Parte 2: fusion de nodos duplicados ----\n")
        w("// Por cada fila: reapuntar TODOS los hijos del nodo perdedor (entrante\n")
        w("// y saliente de HAS_CHILD) al nodo superviviente, luego borrar el perdedor.\n\n")
        for rk in merge_ranks:
            lbl, kp = RANK_META[rk]
            w(f"// fusionar duplicados de rango {rk}\n")
            w("LOAD CSV WITH HEADERS FROM 'file:///dedupe_merge.csv' AS row\n")
            w(f"WITH row WHERE row.loser_rank = '{rk}'\n")
            w(f"MATCH (loser:{lbl} {{{kp}: toInteger(row.loser_key)}})\n")
            w(f"MATCH (survivor:{lbl} {{{kp}: toInteger(row.survivor_key)}})\n")
            w("CALL (loser, survivor) {\n")
            w("  MATCH (loser)-[:HAS_CHILD]->(child)\n")
            w("  MERGE (survivor)-[:HAS_CHILD]->(child)\n")
            w("  RETURN count(*) AS hijos_reapuntados\n")
            w("}\n")
            w("CALL (loser, survivor) {\n")
            w("  MATCH (parent)-[:HAS_CHILD]->(loser)\n")
            w("  MERGE (parent)-[:HAS_CHILD]->(survivor)\n")
            w("  RETURN count(*) AS padres_reapuntados\n")
            w("}\n")
            w("DETACH DELETE loser;\n\n")

        w("// Verificacion: nodos que aun tienen mas de un padre.\n")
        w("MATCH (n) WHERE (n:Phylum OR n:Class OR n:Order OR n:Family OR n:Genus)\n")
        w("  AND size([ (n)<-[:HAS_CHILD]-(z) | z ]) > 1\n")
        w("RETURN labels(n)[0] AS rango, count(*) AS multi ORDER BY multi DESC;\n")
    print(f"escrito {cyp} ({len(combos)} bloques de borrado + {len(merge_ranks)} de fusion)")

    by = defaultdict(int)
    for d in deletes:
        by[(d[1], d[3])] += 1
    print("\nAristas a borrar por combinacion (hijo <- padre espurio):")
    for (cr, pr), n in sorted(by.items(), key=lambda x: -x[1]):
        print(f"  {cr:8s} <- {pr:8s}: {n:,}")

    if merges:
        by_rank = defaultdict(int)
        for (_, r, _, _, _, _) in merges:
            by_rank[r] += 1
        print("\nNodos perdedores a fusionar por rango:")
        for r, n in sorted(by_rank.items(), key=lambda x: -x[1]):
            print(f"  {r:8s}: {n:,}")


if __name__ == '__main__':
    main()