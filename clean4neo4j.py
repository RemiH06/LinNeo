"""
Limpia CSVs con campos de texto para el parser de LOAD CSV de Neo4j.

Quita de cada celda:
- saltos de linea (\\r \\n) -> espacio
- comillas dobles internas -> comillas simples
- espacios multiples -> uno

Uso:
  # Limpiar un archivo especifico (recomendado: directamente el de import/)
  python clean_csvs_for_neo4j.py "C:\\Users\\hecto\\.Neo4jDesktop2\\...\\import\\wikipedia_descriptions.csv"

  # O sin argumentos: limpia los CSV conocidos en biodiversity_data/
  python clean_csvs_for_neo4j.py

Sobreescribe el archivo. Reporta cuantas comillas habia (para confirmar el saneo).
"""

import sys
import re
import csv
import pandas as pd
from pathlib import Path

DEFAULT_TARGETS = [
    "biodiversity_data/descriptions/wikipedia_descriptions.csv",
    "biodiversity_data/descriptions/eol_descriptions.csv",
    "biodiversity_data/powo/powo_plants.csv",
    "biodiversity_data/fishbase/fishbase_fish.csv",
    "biodiversity_data/amphibiaweb/amphibiaweb_data.csv",
]


def sanitize(value):
    if not isinstance(value, str):
        return value
    v = value.replace('\r', ' ').replace('\n', ' ')
    v = v.replace('\\', ' ')          # quitar backslashes (Neo4j los usa como escape)
    v = v.replace('"', "'")           # comillas dobles -> simples
    v = re.sub(r'\s+', ' ', v).strip()
    return v


def clean_file(path: Path):
    if not path.exists():
        print(f"  (omitido, no existe) {path}")
        return
    print(f"Limpiando {path} ...")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)

    # Contar comillas dobles antes (para confirmar)
    quotes_before = 0
    for col in df.columns:
        quotes_before += df[col].str.count('"').sum()

    for col in df.columns:
        df[col] = df[col].map(sanitize)

    df.to_csv(path, index=False, quoting=csv.QUOTE_ALL, encoding='utf-8')
    print(f"  OK ({len(df)} filas). Comillas dobles internas eliminadas: {int(quotes_before)}")


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_TARGETS
    for t in targets:
        clean_file(Path(t))
    print("\nListo.")
    if len(sys.argv) <= 1:
        print("Recuerda volver a copiar los CSV a la carpeta import/ de Neo4j.")