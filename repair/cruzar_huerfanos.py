"""
LinNeo -- Cruce de huerfanos actuales contra los cambios de dedupe_parents.py.

Busca si los nodos huerfanos detectados HOY (export de Neo4j: rango,nombre,key)
coinciden con:
  (a) un parent_key que fue BORRADO en dedupe_delete.csv (es decir, el nodo
      perdio una arista de entrada como padre de alguien -- no aplica aqui,
      eso seria un huerfano de SUS HIJOS, no de el mismo)
  (b) un child_key que aparecio en dedupe_delete.csv (perdio padres, pero
      siempre debia quedarle al menos 1 -- si quedo en 0, hay un bug)
  (c) un loser_key en dedupe_merge.csv que NO se borro como nodo (deberia
      haberse DETACH DELETE, si sigue vivo y huerfano algo fallo a medias)
  (d) un survivor_key en dedupe_merge.csv (recibio hijos reapuntados, no
      deberia quedar huerfano el mismo por este proceso)
  (e) NINGUNA coincidencia -- el huerfano es independiente de los cambios de
      hoy (viene de antes, o de otra causa).

Uso:
  python cruzar_huerfanos.py --huerfanos neo4j_query_table_data_2026-6-23.csv \
                              --delete biodiversity_data/orphan_csv/dedupe_delete.csv \
                              --merge biodiversity_data/orphan_csv/dedupe_merge.csv
"""
import csv, argparse
from collections import defaultdict

csv.field_size_limit(2**31 - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--huerfanos', required=True)
    ap.add_argument('--delete', required=True)
    ap.add_argument('--merge', required=True)
    args = ap.parse_args()

    huerfanos = []  # (rango, nombre, key)
    with open(args.huerfanos, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            huerfanos.append((row['rango'].strip(), row['nombre'].strip(), row['key'].strip()))
    print(f"Huerfanos a revisar: {len(huerfanos):,}")

    del_child_keys = set()    # (key, rank) que aparecieron como child_key en dedupe_delete
    del_parent_keys = set()   # (key, rank) que aparecieron como parent_key (borrado)
    with open(args.delete, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            del_child_keys.add((row['child_key'].strip(), row['child_rank'].strip().lower()))
            del_parent_keys.add((row['parent_key'].strip(), row['parent_rank'].strip().lower()))

    merge_losers = set()      # (key, rank)
    merge_survivors = set()   # (key, rank)
    with open(args.merge, encoding='utf-8-sig', newline='') as f:
        r = csv.DictReader(f)
        for row in r:
            merge_losers.add((row['loser_key'].strip(), row['loser_rank'].strip().lower()))
            merge_survivors.add((row['survivor_key'].strip(), row['survivor_rank'].strip().lower()))

    counts = defaultdict(int)
    detalle_por_causa = defaultdict(list)

    for (rango, nombre, key) in huerfanos:
        rank_l = rango.lower()
        causas = []
        if (key, rank_l) in del_child_keys:
            causas.append('fue child_key en dedupe_delete (perdio padres -- revisar si quedo en 0)')
        if (key, rank_l) in del_parent_keys:
            causas.append('fue parent_key BORRADO en dedupe_delete (alguien le quito una arista de hijo, no afecta sus propios padres)')
        if (key, rank_l) in merge_losers:
            causas.append('fue loser en dedupe_merge (deberia haber sido DETACH DELETE -- si sigue existiendo, no se aplico bien)')
        if (key, rank_l) in merge_survivors:
            causas.append('fue survivor en dedupe_merge (recibio hijos reapuntados)')
        if not causas:
            causas = ['sin coincidencia con los cambios de hoy']
        for c in causas:
            counts[c] += 1
        detalle_por_causa[tuple(causas)].append((rango, nombre, key))

    print("\nResumen de causas:")
    for c, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {n:5,}  {c}")

    # mostrar ejemplos de cada combinacion de causas (hasta 10 por grupo)
    print("\nEjemplos por combinacion de causas:")
    for causas, rows in detalle_por_causa.items():
        print(f"\n--- {' + '.join(causas)} ({len(rows):,} nodos) ---")
        for (rango, nombre, key) in rows[:10]:
            print(f"    {rango:8s} {nombre:30s} key={key}")


if __name__ == '__main__':
    main()