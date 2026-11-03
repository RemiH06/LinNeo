"""
FishBase Fetcher
Descarga datos de peces desde FishBase usando el dump parquet de rOpenSci
(toda la tabla 'species' de una vez, ~36K peces). Rapido: una descarga + proceso local.

Extrae: descripcion (Comments) + habitat (tipo de agua + zona demersal/pelagica).
Matchea con GBIF por nombre cientifico (Genus + Species).

Requiere pyarrow para leer parquet:  pip install pyarrow --break-system-packages

Salida: biodiversity_data/fishbase/fishbase_fish.csv
Columnas: species_key, text, lang, habit, source_name, source_url, accessed_date

Modelo destino:
  (Species)-[:HAS_DESCRIPTION]->(:Description {source_name:"fishbase", text, lang})
  Species.habit = "freshwater; demersal"
"""

import re
import html
import requests
import pandas as pd
import logging
import csv
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime, date

# ==================== CONFIGURACION ====================

# FishBase ahora se aloja en HuggingFace (rfishbase 5.0). Version snapshot:
FB_VERSION = "v25.04"
SPECIES_PARQUET_URL = (
    f"https://huggingface.co/datasets/cboettig/fishbase/resolve/main/"
    f"data/fb/{FB_VERSION}/parquet/species.parquet"
)
MAX_PARAGRAPHS = 2

OUTPUT_DIR = Path("biodiversity_data/fishbase")
OUTPUT_FILE = OUTPUT_DIR / "fishbase_fish.csv"
PARQUET_LOCAL = OUTPUT_DIR / "species.parquet"
LOG_FILE = OUTPUT_DIR / "fishbase_fetcher.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def clean_text(raw) -> str:
    if not isinstance(raw, str) or not raw.strip():
        return ""
    text = re.sub(r'<[^>]+>', '', raw)
    text = html.unescape(text)
    paragraphs = [p.strip() for p in re.split(r'\n+', text) if p.strip()]
    return "\n\n".join(paragraphs[:MAX_PARAGRAPHS]).strip()


def is_true(val) -> bool:
    """FishBase usa -1 (o 1) para verdadero (legacy Access)."""
    return str(val).strip() in ("-1", "1", "-1.0", "1.0", "True", "true")


def build_habit(row) -> str:
    """Combina tipo de agua y zona en un string de habito."""
    waters = []
    if is_true(row.get('Fresh')):
        waters.append("freshwater")
    if is_true(row.get('Brack')):
        waters.append("brackish")
    if is_true(row.get('Saltwater')):
        waters.append("marine")
    zone = row.get('DemersPelag')
    zone = zone.strip() if isinstance(zone, str) else ""
    parts = []
    if waters:
        parts.append("/".join(waters))
    if zone:
        parts.append(zone)
    return "; ".join(parts)


class FishBaseFetcher:
    def __init__(self, output_dir: str = "biodiversity_data/fishbase"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "fishbase_fish.csv"
        self.parquet_path = self.output_dir / "species.parquet"
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "LinNeo/1.0 (https://github.com/RemiH06/LinNeo)"})

    def download_parquet(self) -> bool:
        """Descarga el dump species.parquet si no esta ya en disco."""
        if self.parquet_path.exists():
            logger.info(f"Parquet ya existe: {self.parquet_path}")
            return True
        logger.info(f"Descargando {SPECIES_PARQUET_URL} ...")
        try:
            with self.session.get(SPECIES_PARQUET_URL, stream=True, timeout=300) as r:
                r.raise_for_status()
                with open(self.parquet_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1 << 16):
                        f.write(chunk)
            size_mb = self.parquet_path.stat().st_size / 1024 / 1024
            logger.info(f"Descargado: {size_mb:.1f} MB")
            return True
        except Exception as e:
            logger.error(f"Error descargando parquet: {e}")
            return False

    def load_gbif_name_map(self) -> Dict[str, int]:
        logger.info("Cargando nombres de GBIF...")
        gbif_file = Path("biodiversity_data/gbif_taxonomy.csv")
        if not gbif_file.exists():
            logger.error(f"No se encontro: {gbif_file}")
            return {}
        try:
            df = pd.read_csv(gbif_file, usecols=['species_key', 'scientific_name', 'canonical_name'], low_memory=False)
            name_map = {}
            for key, sci, canon in zip(df['species_key'], df['scientific_name'], df['canonical_name']):
                skey = int(key)
                if pd.notna(canon):
                    name_map[canon.lower().strip()] = skey
                if pd.notna(sci):
                    name_map.setdefault(sci.lower().strip(), skey)
            logger.info(f"Cargados {len(name_map)} nombres de GBIF")
            return name_map
        except Exception as e:
            logger.error(f"Error cargando GBIF: {e}")
            return {}

    def process(self, name_map: Dict[str, int]):
        logger.info("=" * 60)
        logger.info("PROCESANDO FISHBASE (dump parquet)")
        logger.info("=" * 60)

        try:
            df = pd.read_parquet(self.parquet_path)
        except ImportError:
            logger.error("Falta pyarrow. Instala con: pip install pyarrow --break-system-packages")
            return
        except Exception as e:
            logger.error(f"Error leyendo parquet: {e}")
            return

        logger.info(f"Tabla species: {len(df)} filas, {len(df.columns)} columnas")

        accessed = date.today().isoformat()
        records = []
        matched = 0

        for _, row in df.iterrows():
            genus = row.get('Genus')
            species = row.get('Species')
            if not isinstance(genus, str) or not isinstance(species, str):
                continue
            name = f"{genus.strip()} {species.strip()}".lower()
            skey = name_map.get(name)
            if skey is None:
                continue

            text = clean_text(row.get('Comments'))
            habit = build_habit(row)
            if not text and not habit:
                continue

            spec_code = row.get('SpecCode')
            try:
                spec_code = int(spec_code)
                source_url = f"https://www.fishbase.se/summary/{spec_code}.html"
            except (ValueError, TypeError):
                source_url = "https://www.fishbase.se"

            records.append({
                'species_key': skey,
                'text': text,
                'lang': 'en',
                'habit': habit,
                'source_name': 'fishbase',
                'source_url': source_url,
                'accessed_date': accessed,
            })
            matched += 1

        logger.info(f"Peces matcheados con GBIF: {matched}")
        self.save_csv(records)

    def save_csv(self, records):
        if not records:
            logger.warning("Sin registros para guardar")
            return
        with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(['species_key', 'text', 'lang', 'habit', 'source_name', 'source_url', 'accessed_date'])
            for r in records:
                writer.writerow([r['species_key'], r['text'], r['lang'], r['habit'],
                                 r['source_name'], r['source_url'], r['accessed_date']])
        logger.info(f"Guardado: {self.csv_path} ({len(records)} registros)")

        df = pd.DataFrame(records)
        with_text = (df['text'].astype(str).str.len() > 0).sum()
        with_habit = (df['habit'].astype(str).str.len() > 0).sum()
        logger.info(f"Con descripcion: {with_text} | con habitat: {with_habit}")
        logger.info(f"Especies unicas: {df['species_key'].nunique()}")


def fetch_fishbase_data():
    """Funcion principal. Llamada por download_all.py"""
    logger.info(f"Inicio: {datetime.now()}")

    fetcher = FishBaseFetcher()
    if not fetcher.download_parquet():
        return

    name_map = fetcher.load_gbif_name_map()
    if not name_map:
        logger.error("No se pudo cargar GBIF")
        return

    fetcher.process(name_map)
    logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    fetch_fishbase_data()