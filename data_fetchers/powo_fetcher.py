"""
POWO (Plants of the World Online, Kew) Fetcher -- CONCURRENTE
Descarga descripcion + habito de plantas via la API de POWO, en paralelo.

Usa ThreadPoolExecutor: cada worker corre en su propio hilo y hace las 2
requests (search + taxon) de una especie. Como la tarea es de red, los hilos
avanzan en paralelo (el GIL se libera durante la I/O). ~8x mas rapido.

Diseno thread-safe: los workers solo hacen requests y devuelven datos; el hilo
principal es el unico que escribe el CSV y el checkpoint (sin locks).

Compatible con el progreso existente: continua desde donde iba (powo_progress.txt).

Salida: biodiversity_data/powo/powo_plants.csv
Progreso: biodiversity_data/powo/powo_progress.txt
Columnas: species_key, text, lang, habit, source_name, source_url, accessed_date
"""

import re
import html
import threading
import requests
import pandas as pd
import time
import logging
import csv
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, date

# ==================== CONFIGURACION ====================

WORKERS = 8                # hilos concurrentes
BLOCK_SIZE = 400           # especies por bloque (se persiste tras cada bloque)
MAX_PARAGRAPHS = 2

POWO_SEARCH = "https://powo.science.kew.org/api/2/search"
POWO_TAXON = "https://powo.science.kew.org/api/2/taxon/{ipni_id}"

OUTPUT_DIR = Path("biodiversity_data/powo")
OUTPUT_FILE = OUTPUT_DIR / "powo_plants.csv"
PROGRESS_FILE = OUTPUT_DIR / "powo_progress.txt"
LOG_FILE = OUTPUT_DIR / "powo_fetcher.log"

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

# Sesion por hilo (requests.Session no es thread-safe para compartir)
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


def _get_json(url: str, params: dict) -> Optional[dict]:
    session = get_session()
    for attempt in range(4):
        try:
            resp = session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.RequestException, ValueError):
            if attempt < 3:
                time.sleep(2 * (2 ** attempt))
            else:
                return None


def search_ipni_id(name: str) -> Optional[str]:
    data = _get_json(POWO_SEARCH, {"q": name})
    if not data:
        return None
    results = data.get("results", []) or []
    if not results:
        return None
    chosen = None
    for r in results:
        if r.get("accepted") and (r.get("rank", "").upper() == "SPECIES"):
            chosen = r
            break
    if chosen is None:
        chosen = results[0]
    url = chosen.get("url", "")
    if "/taxon/" in url:
        return url.split("/taxon/", 1)[1]
    return chosen.get("fqId")


def get_taxon_data(ipni_id: str) -> Dict:
    data = _get_json(POWO_TAXON.format(ipni_id=ipni_id), {"fields": "descriptions,distribution"})
    if not data:
        return {}

    text = ""
    habit = ""

    descs = data.get("descriptions")
    if isinstance(descs, dict):
        parts = []
        for v in descs.values():
            if isinstance(v, dict):
                parts.append(v.get("description") or v.get("content") or "")
            elif isinstance(v, str):
                parts.append(v)
        text = clean_text(" ".join(p for p in parts if p))
    elif isinstance(descs, list):
        parts = []
        for v in descs:
            if isinstance(v, dict):
                parts.append(v.get("description") or v.get("content") or "")
            elif isinstance(v, str):
                parts.append(v)
        text = clean_text(" ".join(p for p in parts if p))
    elif isinstance(descs, str):
        text = clean_text(descs)

    for key in ("lifeform", "habit", "climate"):
        val = data.get(key)
        if isinstance(val, str) and val.strip():
            habit = val.strip()
            break
        if isinstance(val, dict):
            cand = val.get("description") or val.get("value") or ""
            if cand.strip():
                habit = clean_text(cand)
                break

    return {"text": text, "habit": habit}


def process_one(item: Tuple[int, str]) -> Tuple[int, Optional[Dict]]:
    """Worker: procesa una especie. Devuelve (species_key, record|None)."""
    skey, name = item
    accessed = date.today().isoformat()
    ipni_id = search_ipni_id(name)
    if not ipni_id:
        return (skey, None)
    info = get_taxon_data(ipni_id)
    if info and (info.get("text") or info.get("habit")):
        return (skey, {
            'species_key': skey,
            'text': info.get("text", ""),
            'lang': 'en',
            'habit': info.get("habit", ""),
            'source_name': 'powo',
            'source_url': f"https://powo.science.kew.org/taxon/{ipni_id}",
            'accessed_date': accessed,
        })
    return (skey, None)


class POWOFetcher:
    def __init__(self, output_dir: str = "biodiversity_data/powo"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "powo_plants.csv"
        self.progress_path = self.output_dir / "powo_progress.txt"

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

    def load_gbif_plants(self) -> pd.DataFrame:
        logger.info("Cargando plantas de GBIF...")
        gbif_file = Path("biodiversity_data/gbif_taxonomy.csv")
        if not gbif_file.exists():
            logger.error(f"No se encontro: {gbif_file}")
            return pd.DataFrame()
        try:
            df = pd.read_csv(gbif_file, usecols=['species_key', 'canonical_name', 'kingdom', 'rank'], low_memory=False)
            df = df.dropna(subset=['species_key', 'canonical_name'])
            df = df[df['kingdom'] == 'Plantae']
            if 'rank' in df.columns:
                df = df[df['rank'].astype(str).str.upper() == 'SPECIES']
            df = df.drop_duplicates(subset=['canonical_name'])
            df['species_key'] = df['species_key'].astype(int)
            logger.info(f"Cargadas {len(df)} plantas")
            return df[['species_key', 'canonical_name']].reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error cargando GBIF: {e}")
            return pd.DataFrame()

    def download(self, species_df: pd.DataFrame):
        logger.info("=" * 60)
        logger.info(f"DESCARGANDO PLANTAS DE POWO (concurrente, {WORKERS} hilos)")
        logger.info("=" * 60)

        self._ensure_csv_header()

        processed = self.load_processed()
        if processed:
            species_df = species_df[~species_df['species_key'].isin(processed)]
        pending = list(zip(species_df['species_key'].tolist(), species_df['canonical_name'].tolist()))
        total = len(pending)
        logger.info(f"Pendientes en esta corrida: {total}")
        if total == 0:
            logger.info("Nada pendiente.")
            return

        found = 0
        done = 0

        try:
            with ThreadPoolExecutor(max_workers=WORKERS) as executor:
                for i in range(0, total, BLOCK_SIZE):
                    block = pending[i:i + BLOCK_SIZE]
                    block_records = []
                    block_keys = []

                    # map reparte el bloque entre los 8 hilos
                    for skey, record in executor.map(process_one, block):
                        block_keys.append(skey)
                        if record:
                            block_records.append(record)

                    # Solo el hilo principal persiste (sin locks)
                    self.append_records(block_records)
                    self.mark_processed(block_keys)

                    found += len(block_records)
                    done += len(block_keys)
                    logger.info(f"  Progreso: {done}/{total} | con datos POWO: {found}")

        except KeyboardInterrupt:
            logger.warning("Interrumpido. El progreso del bloque previo quedo guardado; vuelve a correr para retomar.")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)

        logger.info(f"\nLote terminado. Con datos POWO en esta corrida: {found}/{done}")

    def generate_report(self):
        if not self.csv_path.exists():
            return
        try:
            df = pd.read_csv(self.csv_path)
        except Exception:
            return
        logger.info("\n" + "=" * 60)
        logger.info("ESTADISTICAS ACUMULADAS (POWO)")
        logger.info("=" * 60)
        logger.info(f"Total registros POWO: {len(df)}")
        if not df.empty:
            with_text = (df['text'].astype(str).str.len() > 0).sum()
            with_habit = (df['habit'].astype(str).str.len() > 0).sum()
            logger.info(f"Con descripcion: {with_text}")
            logger.info(f"Con habito: {with_habit}")
            logger.info(f"Especies unicas: {df['species_key'].nunique()}")
        logger.info("=" * 60)


def fetch_powo_data():
    """Funcion principal. Llamada por download_all.py"""
    logger.info(f"Inicio: {datetime.now()}")

    fetcher = POWOFetcher()
    species_df = fetcher.load_gbif_plants()
    if species_df.empty:
        logger.error("No hay plantas para procesar")
        return

    fetcher.download(species_df)
    fetcher.generate_report()

    logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    fetch_powo_data()