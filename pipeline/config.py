"""Rutas del pipeline. Son absolutas para no depender del cwd."""
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DATA = REPO / "biodiversity_data"
BACKBONE = DATA / "backbone_extract"
TAXON_TSV = BACKBONE / "Taxon.tsv"

# Salidas de v2. Lo viejo de biodiversity_data/ se borra cuando v2 quede validada.
OUT = DATA / "v2"
TAXA = OUT / "taxa.parquet"
SYNONYMS = OUT / "synonyms.parquet"
ENRICHED = OUT / "enriched"
REJECTS = OUT / "rejects.parquet"
WIKIDATA_DIR = DATA / "wikidata"

# Orden de la jerarquía: un padre siempre tiene nivel menor que su hijo.
RANK_LEVEL = {
    "biota": 0,
    "domain": 1,
    "kingdom": 2,
    "phylum": 3,
    "class": 4,
    "order": 5,
    "family": 6,
    "genus": 7,
    "species": 8,
}
