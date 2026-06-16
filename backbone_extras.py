"""
LinNeo -- Procesa los TSV del backbone de GBIF (ya extraidos) y genera CSV de carga.

NO descarga nada: lee biodiversity_data/backbone_extract/*.tsv y produce CSV en
biodiversity_data/backbone_csv/. El cruce con el grafo se hace en la carga (MATCH),
asi que las especies que no existan como nodo aceptado se ignoran solas.

Fuentes y flags:
  --descriptions : Description.tsv -> descripciones cientificas (tipos narrativos utiles,
                   incluida etymology), todos los idiomas, HTML limpiado.
  --vernacular   : VernacularName.tsv -> nombres comunes en espanol.
  --images       : Multimedia.tsv -> imagenes (png/jpg/jpeg/gif/svg/tif).
  --references   : Reference.tsv -> citas bibliograficas (max N por especie).
  --types        : TypesAndSpecimen.tsv -> especimen tipo (quien lo designo).
  --all          : todo lo anterior.

Modelo destino (cargado por cypher_import_backbone.cypher):
  (Species)-[:HAS_DESCRIPTION]->(:Description {text, type, lang, source_name})
     type in: description, diagnosis, biology, habitat, etymology, discussion, habit,
              reference, type_specimen
  (Species)-[:HAS_MEDIA]->(:Media {media_type:'image', url, title, creator, license, source_name})
  Species.commonNames  (lista; se le agregan los nombres es nuevos, deduplicados)

Uso:
  python process_backbone_extras.py --all
  python process_backbone_extras.py --descriptions --images
"""

import argparse
import csv
import html
import re
from pathlib import Path
import pandas as pd

BACKBONE = Path("biodiversity_data/backbone_extract")
OUT = Path("biodiversity_data/backbone_csv")
OUT.mkdir(parents=True, exist_ok=True)

CHUNK = 500_000

# ── Tipos de descripcion narrativos utiles -> type normalizado ──
DESC_TYPE_MAP = {
    "description": "description", "general": "description", "general description": "description",
    "morphology": "description",
    "diagnosis": "diagnosis",
    "habitat": "habitat",
    "etymology": "etymology",
    "biology_ecology": "biology", "biology": "biology", "ecology": "biology",
    "discussion": "discussion",
    "habit": "habit",
}
# extensiones de imagen aceptadas
IMG_EXT = {"png", "jpg", "jpeg", "gif", "svg", "tif", "tiff"}

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def clean_text(s: str) -> str:
    """Quita HTML, decodifica entidades, normaliza espacios y caracteres problematicos para Neo4j."""
    if not s:
        return ""
    s = TAG_RE.sub(" ", s)          # quitar tags HTML
    s = html.unescape(s)             # &amp; -> &, etc.
    s = s.replace("\\", " ")        # backslash rompe LOAD CSV
    s = s.replace('"', "'")         # comillas dobles -> simples
    s = s.replace("\r", " ").replace("\n", " ")
    s = WS_RE.sub(" ", s).strip()
    return s


def process_descriptions():
    src = BACKBONE / "Description.tsv"
    if not src.exists():
        print(f"  [descriptions] no existe {src}"); return
    print("Procesando Description.tsv...")
    out_path = OUT / "backbone_descriptions.csv"
    seen = set()  # (species_key, type, hash(text)) para deduplicar
    n_in = n_out = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(["species_key", "type", "language", "text", "source_name"])
        for chunk in pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                                 usecols=["taxonID", "type", "language", "description", "source"],
                                 chunksize=CHUNK):
            n_in += len(chunk)
            chunk["t"] = chunk["type"].str.lower().str.strip().map(DESC_TYPE_MAP)
            chunk = chunk[chunk["t"].notna()]
            for r in chunk.itertuples(index=False):
                key = r.taxonID.strip()
                if not key:
                    continue
                text = clean_text(r.description)
                if len(text) < 10:        # descartar fragmentos triviales ("Fig. 3 A")
                    continue
                dedup = (key, r.t, hash(text[:120]))
                if dedup in seen:
                    continue
                seen.add(dedup)
                w.writerow([key, r.t, (r.language or "").strip().lower(), text, clean_text(r.source)])
                n_out += 1
    print(f"  leidas {n_in:,} filas -> {n_out:,} descripciones utiles -> {out_path}")


def process_vernacular():
    src = BACKBONE / "VernacularName.tsv"
    if not src.exists():
        print(f"  [vernacular] no existe {src}"); return
    print("Procesando VernacularName.tsv (espanol)...")
    out_path = OUT / "backbone_vernacular_es.csv"
    seen = set()
    n_out = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(["species_key", "vernacular_name"])
        for chunk in pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                                 usecols=["taxonID", "vernacularName", "language"],
                                 chunksize=CHUNK):
            es = chunk[chunk["language"].str.lower().str.strip() == "es"]
            for r in es.itertuples(index=False):
                key = r.taxonID.strip()
                name = clean_text(r.vernacularName)
                if not key or not name:
                    continue
                d = (key, name.lower())
                if d in seen:
                    continue
                seen.add(d)
                w.writerow([key, name])
                n_out += 1
    print(f"  -> {n_out:,} nombres comunes en espanol -> {out_path}")


def process_images():
    src = BACKBONE / "Multimedia.tsv"
    if not src.exists():
        print(f"  [images] no existe {src}"); return
    print("Procesando Multimedia.tsv (imagenes)...")
    out_path = OUT / "backbone_images.csv"
    n_out = 0
    ext_re = re.compile(r"\.([a-zA-Z0-9]{2,4})(?:\?|$)")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(["species_key", "url", "title", "creator", "license", "source_name"])
        for chunk in pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                                 usecols=["taxonID", "identifier", "title", "creator", "license", "source"],
                                 chunksize=CHUNK):
            for r in chunk.itertuples(index=False):
                key = r.taxonID.strip()
                url = (r.identifier or "").strip()
                if not key or not url:
                    continue
                m = ext_re.search(url)
                ext = m.group(1).lower() if m else ""
                if ext not in IMG_EXT:
                    continue
                w.writerow([key, url.replace('"', "%22"), clean_text(r.title),
                            clean_text(r.creator), clean_text(r.license), clean_text(r.source)])
                n_out += 1
    print(f"  -> {n_out:,} imagenes -> {out_path}")


def process_references(max_per_species: int = 5):
    src = BACKBONE / "Reference.tsv"
    if not src.exists():
        print(f"  [references] no existe {src}"); return
    print(f"Procesando Reference.tsv (max {max_per_species} por especie)...")
    out_path = OUT / "backbone_references.csv"
    count = {}
    n_out = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(["species_key", "text", "source_name"])
        for chunk in pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                                 usecols=["taxonID", "bibliographicCitation", "source"],
                                 chunksize=CHUNK):
            for r in chunk.itertuples(index=False):
                key = r.taxonID.strip()
                cite = clean_text(r.bibliographicCitation)
                if not key or len(cite) < 10:
                    continue
                if count.get(key, 0) >= max_per_species:
                    continue
                count[key] = count.get(key, 0) + 1
                w.writerow([key, cite, clean_text(r.source)])
                n_out += 1
    print(f"  -> {n_out:,} referencias -> {out_path}")


def process_types():
    src = BACKBONE / "TypesAndSpecimen.tsv"
    if not src.exists():
        print(f"  [types] no existe {src}"); return
    print("Procesando TypesAndSpecimen.tsv...")
    out_path = OUT / "backbone_types.csv"
    seen = set()
    n_out = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(["species_key", "text", "source_name"])
        for chunk in pd.read_csv(src, sep="\t", dtype=str, keep_default_na=False,
                                 usecols=["taxonID", "typeDesignatedBy", "scientificName", "source"],
                                 chunksize=CHUNK):
            for r in chunk.itertuples(index=False):
                key = r.taxonID.strip()
                by = clean_text(r.typeDesignatedBy)
                sci = clean_text(r.scientificName)
                if not key or (not by and not sci):
                    continue
                # texto legible del espicimen tipo
                parts = []
                if sci: parts.append(sci)
                if by: parts.append(f"designado por {by}")
                text = "; ".join(parts)
                d = (key, text.lower())
                if d in seen:
                    continue
                seen.add(d)
                w.writerow([key, text, clean_text(r.source)])
                n_out += 1
    print(f"  -> {n_out:,} especimenes tipo -> {out_path}")


def main():
    ap = argparse.ArgumentParser(description="Procesa TSV del backbone GBIF a CSV de carga")
    ap.add_argument("--descriptions", action="store_true")
    ap.add_argument("--vernacular", action="store_true")
    ap.add_argument("--images", action="store_true")
    ap.add_argument("--references", action="store_true")
    ap.add_argument("--types", action="store_true")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--refs-per-species", type=int, default=5)
    args = ap.parse_args()

    if args.all or args.descriptions: process_descriptions()
    if args.all or args.vernacular:   process_vernacular()
    if args.all or args.images:       process_images()
    if args.all or args.references:   process_references(args.refs_per_species)
    if args.all or args.types:        process_types()

    if not any([args.all, args.descriptions, args.vernacular, args.images, args.references, args.types]):
        ap.print_help()


if __name__ == "__main__":
    main()