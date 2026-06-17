"""
LinNeo -- Explorador de Taxon.tsv (GBIF backbone)
Inspecciona estructura y contenido sin cargar los 2.2GB en memoria.

Uso (PowerShell o terminal, dentro de la raiz del proyecto):
  python explore_taxon.py header
      -> muestra los nombres de columna (encabezado)

  python explore_taxon.py sample 5
      -> muestra las primeras 5 filas completas, columna por columna

  python explore_taxon.py find "Amanita muscaria"
      -> busca las primeras filas cuyo scientificName/canonicalName contenga el texto

  python explore_taxon.py fungi 5
      -> muestra 5 filas de ejemplo donde kingdom == 'Fungi', por cada rango (kingdom..genus)

  python explore_taxon.py ranks-fungi
      -> cuenta cuantos nodos Fungi hay por cada taxonRank (recorre el archivo, tarda)

  python explore_taxon.py keycols
      -> reporta que columnas de "key" existen (phylumKey, classKey, etc.) -- CLAVE para reconstruir relaciones

Por defecto busca el archivo en:
  biodiversity_data/backbone_extract/Taxon.tsv
Puedes pasar otra ruta con  --taxon RUTA
"""
import csv, sys, argparse

# Taxon.tsv tiene campos muy largos; subir el limite del modulo csv
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

DEFAULT = 'biodiversity_data/backbone_extract/Taxon.tsv'

def get_header(path):
    with open(path, encoding='utf-8') as f:
        return f.readline().rstrip('\n').rstrip('\r').split('\t')

def cmd_header(path, _):
    h = get_header(path)
    print(f"\n{len(h)} columnas:\n")
    for i, c in enumerate(h):
        print(f"  [{i:2}] {c}")

def cmd_sample(path, args):
    n = int(args[0]) if args else 3
    h = get_header(path)
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        for r, parts in enumerate(reader):
            if r >= n:
                break
            print(f"\n=== Fila {r+1} ===")
            for i, c in enumerate(h):
                val = parts[i] if i < len(parts) else ''
                if val:
                    print(f"  {c}: {val}")

def cmd_find(path, args):
    needle = args[0] if args else ''
    h = get_header(path)
    # columnas de nombre
    name_cols = [h.index(c) for c in ('scientificName', 'canonicalName') if c in h]
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        found = 0
        for parts in reader:
            if any(i < len(parts) and needle.lower() in parts[i].lower() for i in name_cols):
                print(f"\n=== Coincidencia {found+1} ===")
                for i, c in enumerate(h):
                    val = parts[i] if i < len(parts) else ''
                    if val:
                        print(f"  {c}: {val}")
                found += 1
                if found >= 3:
                    break
        if not found:
            print(f"Sin coincidencias para '{needle}'")

def cmd_byid(path, args):
    """Busca filas cuyo taxonID sea EXACTAMENTE el valor dado (sin substring)."""
    target = args[0] if args else ''
    h = get_header(path)
    ti = h.index('taxonID') if 'taxonID' in h else None
    if ti is None:
        print("No hay columna taxonID")
        return
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        for parts in reader:
            if ti < len(parts) and parts[ti] == target:
                print(f"\n=== taxonID {target} encontrado ===")
                for i, c in enumerate(h):
                    val = parts[i] if i < len(parts) else ''
                    if val:
                        print(f"  {c}: {val}")
                return
    print(f"taxonID {target} NO existe en el TSV")

def cmd_byname(path, args):
    """Busca filas cuyo canonicalName/scientificName sea EXACTAMENTE el valor dado."""
    target = (args[0] if args else '').strip()
    h = get_header(path)
    name_cols = [(c, h.index(c)) for c in ('canonicalName', 'scientificName') if c in h]
    found = 0
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        for parts in reader:
            if any(i < len(parts) and parts[i] == target for _, i in name_cols):
                print(f"\n=== Coincidencia exacta {found+1} ===")
                for i, c in enumerate(h):
                    val = parts[i] if i < len(parts) else ''
                    if val:
                        print(f"  {c}: {val}")
                found += 1
                if found >= 8:
                    break
    if not found:
        print(f"Ningun taxon con nombre exacto '{target}'")
    else:
        print(f"\nTotal coincidencias exactas mostradas: {found}")

def cmd_fungi(path, args):
    n = int(args[0]) if args else 3
    h = get_header(path)
    ki = h.index('kingdom') if 'kingdom' in h else None
    ri = h.index('taxonRank') if 'taxonRank' in h else None
    if ki is None or ri is None:
        print("No hay columnas kingdom/taxonRank")
        return
    seen = {}
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        for parts in reader:
            if ki >= len(parts) or parts[ki] != 'Fungi':
                continue
            rank = parts[ri].lower() if ri < len(parts) else ''
            if rank in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus'):
                seen.setdefault(rank, [])
                if len(seen[rank]) < n:
                    seen[rank].append(parts)
            if all(len(seen.get(r, [])) >= n for r in ('phylum', 'class', 'order', 'family', 'genus')):
                break
    for rank in ('kingdom', 'phylum', 'class', 'order', 'family', 'genus'):
        rows = seen.get(rank, [])
        print(f"\n===== {rank.upper()} ({len(rows)} ejemplos) =====")
        for parts in rows:
            vals = {c: (parts[i] if i < len(parts) else '') for i, c in enumerate(h)}
            shown = {k: v for k, v in vals.items() if v}
            print(" ", shown)

def cmd_keycols(path, _):
    h = get_header(path)
    keycols = [c for c in h if c.lower().endswith('key') or c.lower().endswith('id')]
    print("\nColumnas de tipo key/id (sirven para reconstruir relaciones):")
    for c in keycols:
        print(f"  - {c}")
    rankcols = [c for c in ('kingdom','phylum','class','order','family','genus') if c in h]
    print("\nColumnas de nombre de rango presentes:")
    for c in rankcols:
        print(f"  - {c}")
    print("\nPista: si existen phylumKey/classKey/orderKey/familyKey/genusKey, la reconstruccion")
    print("de relaciones es directa. Si no, usaremos taxonID + parentNameUsageID.")

def cmd_ranks_fungi(path, _):
    h = get_header(path)
    ki = h.index('kingdom') if 'kingdom' in h else None
    ri = h.index('taxonRank') if 'taxonRank' in h else None
    counts = {}
    n = 0
    with open(path, encoding='utf-8') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader, None)
        for parts in reader:
            n += 1
            if n % 1000000 == 0:
                print(f"  ...{n:,} filas")
            if ki is not None and ki < len(parts) and parts[ki] == 'Fungi':
                rank = parts[ri].lower() if ri < len(parts) else '?'
                counts[rank] = counts.get(rank, 0) + 1
    print("\nNodos Fungi por rango:")
    for r, c in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {r}: {c:,}")

CMDS = {
    'header': cmd_header, 'sample': cmd_sample, 'find': cmd_find,
    'byid': cmd_byid, 'byname': cmd_byname,
    'fungi': cmd_fungi, 'keycols': cmd_keycols, 'ranks-fungi': cmd_ranks_fungi,
}

if __name__ == '__main__':
    argv = sys.argv[1:]
    taxon = DEFAULT
    if '--taxon' in argv:
        i = argv.index('--taxon')
        taxon = argv[i+1]
        argv = argv[:i] + argv[i+2:]
    if not argv or argv[0] not in CMDS:
        print(__doc__)
        sys.exit(0)
    cmd = argv[0]
    try:
        CMDS[cmd](taxon, argv[1:])
    except FileNotFoundError:
        print(f"ERROR: no se encuentra {taxon}")
        print("Pasa la ruta con --taxon RUTA  o  corre desde la raiz del proyecto.")