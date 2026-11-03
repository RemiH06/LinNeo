"""Etapa 2: baja de Wikidata el puente de IDs item <-> GBIF (P846) y sus artículos.

Es lo que permite casar por ID: un artículo de Wikipedia lleva a su item y el
item al taxonID, sin pasar por el nombre.

Son consultas separadas porque con OPTIONAL el servidor corta a medio resultado
y aun así responde 200: el aviso llega como última línea del TSV. Por eso cada
respuesta se valida antes de publicarla.

    python pipeline/02_wikidata_ids.py
"""
import json
import shutil
import sys
import urllib.parse
import urllib.request

from config import WIKIDATA_DIR

ENDPOINT = "https://qlever.dev/api/wikidata"
PREFIXES = """
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX schema: <http://schema.org/>
"""
SITELINK = """SELECT ?item ?gbif ?title WHERE {{
  ?item wdt:P846 ?gbif .
  ?title schema:about ?item ; schema:isPartOf <https://{lang}.wikipedia.org/> .
}}"""
QUERIES = {
    "gbif_ids.tsv": "SELECT ?item ?gbif WHERE { ?item wdt:P846 ?gbif }",
    "gbif_en.tsv": SITELINK.format(lang="en"),
    "gbif_es.tsv": SITELINK.format(lang="es"),
}
COUNT = "SELECT (COUNT(*) AS ?n) WHERE { ?item wdt:P846 ?gbif }"


def post(query, accept):
    data = urllib.parse.urlencode({"query": PREFIXES + query}).encode()
    headers = {"Accept": accept, "User-Agent": "LinNeo-pipeline"}
    request = urllib.request.Request(ENDPOINT, data=data, headers=headers)
    return urllib.request.urlopen(request, timeout=600)


def download(name, query):
    """Escribe a un .tmp y regresa cuántas filas de datos llegaron."""
    tmp = (WIKIDATA_DIR / name).with_suffix(".tmp")
    with post(query, "text/tab-separated-values") as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out)
    columns = len(query.split("WHERE")[0].split("?")) - 1
    rows = 0
    with open(tmp, encoding="utf-8") as lines:
        next(lines)
        for line in lines:
            if line.count("\t") != columns - 1:
                sys.exit(f"{name} truncado en la fila {rows}: {line.strip()[:80]}")
            rows += 1
    return tmp, rows


def main():
    WIKIDATA_DIR.mkdir(parents=True, exist_ok=True)
    with post(COUNT, "application/sparql-results+json") as response:
        expected = int(json.load(response)["results"]["bindings"][0]["n"]["value"])

    downloads = {}
    for name, query in QUERIES.items():
        downloads[name], rows = download(name, query)
        print(f"{name}: {rows:,} filas")
        if name == "gbif_ids.tsv" and rows != expected:
            sys.exit(f"{name}: esperaba {expected:,} filas, llegaron {rows:,}")

    # Se publican juntos: nunca queda un puente mezclado de dos descargas.
    for name, tmp in downloads.items():
        tmp.replace(WIKIDATA_DIR / name)
    print(f"Escrito en {WIKIDATA_DIR}")


if __name__ == "__main__":
    main()
