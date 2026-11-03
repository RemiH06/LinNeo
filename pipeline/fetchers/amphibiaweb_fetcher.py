"""
AmphibiaWeb Fetcher -- CONCURRENTE
Descarga descripciones de anfibios desde el web service XML de AmphibiaWeb.

Endpoint: https://amphibiaweb.org/cgi/amphib_ws?where-genus=X&where-species=Y&src=linneo
Devuelve XML con el "species account" (descripcion, distribucion, historia natural).

Solo class=Amphibia (GBIF), rank species (~9000 especies).
Concurrente con ThreadPoolExecutor (8 hilos): termina en minutos.

Salida: biodiversity_data/amphibiaweb/amphibiaweb_data.csv
Progreso: biodiversity_data/amphibiaweb/amphibiaweb_progress.txt
Columnas: species_key, text, lang, habit, source_name, source_url, accessed_date

Modelo destino:
  (Species)-[:HAS_DESCRIPTION]->(:Description {source_name:"amphibiaweb", text, lang})
"""

import re
import html
import threading
import requests
import pandas as pd
import time
import logging
import csv
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, date

# ==================== CONFIGURACION ====================

WORKERS = 8
BLOCK_SIZE = 200
MAX_PARAGRAPHS = 2

AMPHIB_WS = "https://amphibiaweb.org/cgi/amphib_ws"

# Tags del XML cuyo texto nos interesa (match parcial, sin namespace)
DESC_TAGS = ("description", "diagnosis", "natural", "life", "distribution", "comment")

OUTPUT_DIR = Path("biodiversity_data/amphibiaweb")
OUTPUT_FILE = OUTPUT_DIR / "amphibiaweb_data.csv"
PROGRESS_FILE = OUTPUT_DIR / "amphibiaweb_progress.txt"
LOG_FILE = OUTPUT_DIR / "amphibiaweb_fetcher.log"

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

_thread_local = threading.local()


def get_session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": "LinNeo/1.0 (https://github.com/RemiH06/LinNeo)"})
        _thread_local.session = s
    return s


def clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r'<[^>]+>', '', raw)
    text = html.unescape(text)
    paragraphs = [p.strip() for p in re.split(r'\n+', text) if p.strip()]
    return "\n\n".join(paragraphs[:MAX_PARAGRAPHS]).strip()


def local_tag(tag: str) -> str:
    """Quita el namespace de un tag XML."""
    return tag.split('}')[-1].lower() if tag else ""


def process_one(item: Tuple[int, str, str]) -> Tuple[int, Optional[Dict]]:
    """Worker: consulta AmphibiaWeb para una especie. Devuelve (skey, record|None)."""
    skey, genus, species = item
    accessed = date.today().isoformat()
    session = get_session()
    params = {"where-genus": genus, "where-species": species, "src": "linneo"}

    data = None
    for attempt in range(4):
        try:
            resp = session.get(AMPHIB_WS, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.content
            break
        except requests.exceptions.RequestException:
            if attempt < 3:
                time.sleep(2 * (2 ** attempt))
            else:
                return (skey, None)

    if not data:
        return (skey, None)

    try:
        root = ET.fromstring(data)
    except ET.ParseError:
        return (skey, None)

    # Recorrer el arbol y juntar texto de los elementos descriptivos
    parts = []
    amphib_id = None
    for elem in root.iter():
        tag = local_tag(elem.tag)
        if tag in ("amphib_id", "id") and elem.text and elem.text.strip().isdigit():
            amphib_id = elem.text.strip()
        if any(k in tag for k in DESC_TAGS):
            if elem.text and elem.text.strip():
                parts.append(elem.text.strip())

    text = clean_text(" \n".join(parts))
    if len(text) < 50:
        return (skey, None)

    if amphib_id:
        source_url = f"https://amphibiaweb.org/species/{amphib_id}"
    else:
        source_url = f"https://amphibiaweb.org/cgi/amphib_query?where-genus={genus}&where-species={species}"

    return (skey, {
        'species_key': skey,
        'text': text,
        'lang': 'en',
        'habit': '',
        'source_name': 'amphibiaweb',
        'source_url': source_url,
        'accessed_date': accessed,
    })


class AmphibiaWebFetcher:
    def __init__(self, output_dir: str = "biodiversity_data/amphibiaweb"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "amphibiaweb_data.csv"
        self.progress_path = self.output_dir / "amphibiaweb_progress.txt"

    def load_processed(self) -> Set[int]:
        if not self.progress_path.exists():
            return set()
        try:
            with open(self.progress_path, 'r', encoding='utf-8') as f:
                return {int(line.strip()) for line in f if line.strip()}
        except Exception:
            return set()

    def _ensure_csv_header(self):
        if not self.csv_path.exists():
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(['species_key', 'text', 'lang', 'habit', 'source_name', 'source_url', 'accessed_date'])

    def append_records(self, records: List[Dict]):
        if not records:
            return
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            for r in records:
                writer.writerow([r['species_key'], r['text'], r['lang'], r['habit'],
                                 r['source_name'], r['source_url'], r['accessed_date']])

    def mark_processed(self, keys: List[int]):
        with open(self.progress_path, 'a', encoding='utf-8') as f:
            for k in keys:
                f.write(f"{k}\n")

    def load_gbif_amphibians(self) -> pd.DataFrame:
        logger.info("Cargando anfibios de GBIF...")
        gbif_file = Path("biodiversity_data/gbif_taxonomy.csv")
        if not gbif_file.exists():
            logger.error(f"No se encontro: {gbif_file}")
            return pd.DataFrame()
        try:
            df = pd.read_csv(gbif_file, usecols=['species_key', 'canonical_name', 'class', 'rank'], low_memory=False)
            df = df.dropna(subset=['species_key', 'canonical_name'])
            df = df[df['class'] == 'Amphibia']
            if 'rank' in df.columns:
                df = df[df['rank'].astype(str).str.upper() == 'SPECIES']
            df = df.drop_duplicates(subset=['canonical_name'])
            df['species_key'] = df['species_key'].astype(int)
            logger.info(f"Cargados {len(df)} anfibios")
            return df[['species_key', 'canonical_name']].reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error cargando GBIF: {e}")
            return pd.DataFrame()

    def download(self, species_df: pd.DataFrame):
        logger.info("=" * 60)
        logger.info(f"DESCARGANDO ANFIBIOS DE AMPHIBIAWEB ({WORKERS} hilos)")
        logger.info("=" * 60)

        self._ensure_csv_header()

        processed = self.load_processed()
        if processed:
            species_df = species_df[~species_df['species_key'].isin(processed)]

        # Preparar items (skey, genus, species)
        items = []
        for skey, name in zip(species_df['species_key'].tolist(), species_df['canonical_name'].tolist()):
            parts = name.split()
            if len(parts) >= 2:
                items.append((int(skey), parts[0], parts[1]))

        total = len(items)
        logger.info(f"Pendientes en esta corrida: {total}")
        if total == 0:
            logger.info("Nada pendiente.")
            return

        found = 0
        done = 0

        try:
            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                for i in range(0, total, BLOCK_SIZE):
                    block = items[i:i + BLOCK_SIZE]
                    block_records = []
                    block_keys = []

                    for skey, record in executor.map(process_one, block):
                        block_keys.append(skey)
                        if record:
                            block_records.append(record)

                    self.append_records(block_records)
                    self.mark_processed(block_keys)

                    found += len(block_records)
                    done += len(block_keys)
                    logger.info(f"  Progreso: {done}/{total} | con descripcion: {found}")

        except KeyboardInterrupt:
            logger.warning("Interrumpido. El progreso quedo guardado; vuelve a correr para retomar.")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)

        logger.info(f"\nLote terminado. Con descripcion en esta corrida: {found}/{done}")

    def generate_report(self):
        if not self.csv_path.exists():
            return
        try:
            df = pd.read_csv(self.csv_path)
        except Exception:
            return
        logger.info("\n" + "=" * 60)
        logger.info("ESTADISTICAS ACUMULADAS (AmphibiaWeb)")
        logger.info("=" * 60)
        logger.info(f"Total descripciones: {len(df)}")
        if not df.empty:
            logger.info(f"Especies unicas: {df['species_key'].nunique()}")
        logger.info("=" * 60)


def fetch_amphibiaweb_data():
    """Funcion principal. Llamada por download_all.py"""
    logger.info(f"Inicio: {datetime.now()}")

    fetcher = AmphibiaWebFetcher()
    species_df = fetcher.load_gbif_amphibians()
    if species_df.empty:
        logger.error("No hay anfibios para procesar")
        return

    fetcher.download(species_df)
    fetcher.generate_report()

    logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    fetch_amphibiaweb_data()