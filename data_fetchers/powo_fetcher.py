"""
POWO (Plants of the World Online, Kew) Fetcher
Descarga descripcion + habito de plantas via la API de POWO.

Flujo (API publica de POWO, sin token):
  1. search: nombre -> IPNI ID   (https://powo.science.kew.org/api/2/search)
  2. taxon:  IPNI ID -> datos     (https://powo.science.kew.org/api/2/taxon/{id})
Son 2 requests por especie (lento); por eso usa checkpoint.

Solo reino Plantae, rank species.

Salida: biodiversity_data/powo/powo_plants.csv
Progreso: biodiversity_data/powo/powo_progress.txt
Columnas: species_key, text, lang, habit, source_name, source_url, accessed_date

Modelo destino:
  (Species)-[:HAS_DESCRIPTION]->(:Description {source_name:"powo", text, lang})
  Species.habit = "tree/herb/..."  (propiedad directa, filtrable)
"""

import re
import html
import requests
import pandas as pd
import time
import logging
import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime, date

# ==================== CONFIGURACION ====================

MAX_PARAGRAPHS = 2
POWO_SEARCH = "https://powo.science.kew.org/api/2/search"
POWO_TAXON = "https://powo.science.kew.org/api/2/taxon/{ipni_id}"

OUTPUT_DIR = Path("biodiversity_data/powo")
OUTPUT_FILE = OUTPUT_DIR / "powo_plants.csv"
PROGRESS_FILE = OUTPUT_DIR / "powo_progress.txt"
LOG_FILE = OUTPUT_DIR / "powo_fetcher.log"

FLUSH_EVERY = 50

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


def clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = re.sub(r'<[^>]+>', '', raw)
    text = html.unescape(text)
    paragraphs = [p.strip() for p in re.split(r'\n+', text) if p.strip()]
    return "\n\n".join(paragraphs[:MAX_PARAGRAPHS]).strip()


class POWOFetcher:
    """Descarga descripcion + habito de plantas desde POWO, con resume."""

    def __init__(self, output_dir: str = "biodiversity_data/powo"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "powo_plants.csv"
        self.progress_path = self.output_dir / "powo_progress.txt"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LinNeo/1.0 (https://github.com/RemiH06/LinNeo)"
        })

    # ---------- Checkpoint ----------

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

    def append_record(self, r: Dict):
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow([r['species_key'], r['text'], r['lang'], r['habit'],
                             r['source_name'], r['source_url'], r['accessed_date']])

    def mark_processed(self, key: int):
        with open(self.progress_path, 'a', encoding='utf-8') as f:
            f.write(f"{key}\n")

    # ---------- Carga de especies ----------

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

    # ---------- POWO API ----------

    def _get_json(self, url: str, params: dict) -> Optional[dict]:
        for attempt in range(4):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                wait = 3 * (2 ** attempt)
                if attempt < 3:
                    time.sleep(wait)
                else:
                    logger.warning(f"Fallo request POWO: {e}")
                    return None

    def search_ipni_id(self, name: str) -> Optional[str]:
        """Busca el nombre y devuelve el IPNI ID del primer resultado."""
        data = self._get_json(POWO_SEARCH, {"q": name})
        if not data:
            return None
        results = data.get("results", []) or []
        if not results:
            return None
        # Preferir un resultado aceptado a nivel especie
        chosen = None
        for r in results:
            if r.get("accepted") and (r.get("rank", "").upper() == "SPECIES"):
                chosen = r
                break
        if chosen is None:
            chosen = results[0]
        # El IPNI ID viene en 'url' ("/taxon/urn:lsid:ipni.org:names:XXXX-Y") o 'fqId'
        url = chosen.get("url", "")
        if "/taxon/" in url:
            return url.split("/taxon/", 1)[1]
        return chosen.get("fqId")

    def get_taxon_data(self, ipni_id: str) -> Dict:
        """Devuelve {'text':..., 'habit':...} del taxon."""
        data = self._get_json(POWO_TAXON.format(ipni_id=ipni_id), {"fields": "descriptions,distribution"})
        if not data:
            return {}

        text = ""
        habit = ""

        # Descripcion: POWO puede traerla en varias formas
        descs = data.get("descriptions")
        if isinstance(descs, dict):
            # dict por seccion: {"general": {"description": "..."}, ...}
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

        # Habito: campos candidatos en POWO
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

    # ---------- Descarga ----------

    def download(self, species_df: pd.DataFrame):
        logger.info("=" * 60)
        logger.info("DESCARGANDO PLANTAS DE POWO (descripcion + habito)")
        logger.info("=" * 60)

        accessed = date.today().isoformat()
        self._ensure_csv_header()

        processed = self.load_processed()
        if processed:
            species_df = species_df[~species_df['species_key'].isin(processed)]
        pending = species_df.reset_index(drop=True)
        total = len(pending)
        logger.info(f"Pendientes en esta corrida: {total}")
        if total == 0:
            logger.info("Nada pendiente.")
            return

        found = 0
        done = 0

        try:
            for _, row in pending.iterrows():
                skey = int(row['species_key'])
                name = row['canonical_name']

                ipni_id = self.search_ipni_id(name)
                time.sleep(0.4)
                if ipni_id:
                    info = self.get_taxon_data(ipni_id)
                    if info and (info.get("text") or info.get("habit")):
                        self.append_record({
                            'species_key': skey,
                            'text': info.get("text", ""),
                            'lang': 'en',
                            'habit': info.get("habit", ""),
                            'source_name': 'powo',
                            'source_url': f"https://powo.science.kew.org/taxon/{ipni_id}",
                            'accessed_date': accessed,
                        })
                        found += 1
                    time.sleep(0.4)

                self.mark_processed(skey)
                done += 1
                if done % FLUSH_EVERY == 0 or done >= total:
                    logger.info(f"  Progreso: {done}/{total} | con datos POWO: {found}")

        except KeyboardInterrupt:
            logger.warning("Interrumpido. El progreso quedo guardado; vuelve a correr para retomar.")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)

        logger.info(f"\nLote terminado. Con datos POWO en esta corrida: {found}/{total}")

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