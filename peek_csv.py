"""
Muestra los bytes alrededor de una posicion en un archivo, para diagnosticar
errores de parseo de CSV en Neo4j.

Uso:
  python peek_csv.py "RUTA_AL_CSV" 192264884
"""
import sys

path = sys.argv[1]
pos = int(sys.argv[2])
window = 400

with open(path, 'rb') as f:
    start = max(0, pos - window)
    f.seek(start)
    chunk = f.read(window * 2)

print(f"--- bytes {start} a {start + len(chunk)} ---")
print(repr(chunk.decode('utf-8', errors='replace')))
print("\n--- conteo de comillas dobles en esta ventana ---")
print(chunk.count(b'"'))