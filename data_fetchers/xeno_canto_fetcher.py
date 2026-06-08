"""
Xeno-canto Sounds Fetcher
Descarga UNA grabacion por especie desde Xeno-canto (API v3), cubriendo
TODOS los grupos: aves, saltamontes, murcielagos, ranas, mamiferos terrestres.

Enfoque: en vez de iterar especies de GBIF (la mayoria no tiene sonido),
pagina cada grupo de Xeno-canto y se queda con la primera grabacion de cada
especie, matcheando el nombre cientifico contra GBIF. Asi cubre todas las
especies que XC tiene, con un solo sonido cada una.

NOTA: La API v3 requiere API KEY gratuita (desde oct 2025).
  1. Registrate en https://xeno-canto.org/
  2. Verifica tu email y obten la key en tu Account Page
  3. Ponla en XENO_CANTO_API_KEY (variable de entorno o archivo .secrets/.env)

Salida: biodiversity_data/xeno_canto/xeno_canto_sounds.csv
Progreso: biodiversity_data/xeno_canto/xeno_canto_progress.json
Columnas: species_key, url, media_type, source_name, source_url, license, accessed_date

Modelo destino (opcion B):
  (Species)-[:HAS_MEDIA]->(:Media {media_type:"sound", url, source_name, source_url, license})
"""

import os
import json
import requests
import pandas as pd
import time
import logging
import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime, date

# ==================== CONFIGURACION ====================

# Grupos de Xeno-canto (identificadores tal como en la web)
GROUPS = ["birds", "grasshoppers", "bats", "frogs", "land mammals"]

XENO_CANTO_API = "https://xeno-canto.org/api/3/recordings"

OUTPUT_DIR = Path("biodiversity_data/xeno_canto")
OUTPUT_FILE = OUTPUT_DIR / "xeno_canto_sounds.csv"
PROGRESS_FILE = OUTPUT_DIR / "xeno_canto_progress.json"
LOG_FILE = OUTPUT_DIR / "xeno_canto_fetcher.log"

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


def get_api_key() -> str:
    """Lee la API key de variable de entorno o del archivo .secrets/.env."""
    key = os.getenv("XENO_CANTO_API_KEY", "").strip()
    if key:
        return key
    for fname in [".secrets", ".env"]:
        p = Path(fname)
        if p.exists():
            for line in p.read_text(encoding="utf-8").splitlines():
                if line.strip().startswith("XENO_CANTO_API_KEY"):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


class XenoCantoFetcher:
    """Descarga un sonido por especie recorriendo los grupos de Xeno-canto."""

    def __init__(self, api_key: str, output_dir: str = "biodiversity_data/xeno_canto"):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "xeno_canto_sounds.csv"
        self.progress_path = self.output_dir / "xeno_canto_progress.json"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LinNeo/1.0 (https://github.com/RemiH06/LinNeo)"
        })

    # ---------- Checkpoint ----------

    def load_progress(self) -> Dict[str, int]:
        """{grupo: ultima_pagina_completada}"""
        if not self.progress_path.exists():
            return {}
        try:
            return json.loads(self.progress_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_progress(self, progress: Dict[str, int]):
        self.progress_path.write_text(json.dumps(progress), encoding="utf-8")

    def load_seen_species(self) -> Set[int]:
        """species_key ya guardados (del CSV), para no duplicar."""
        if not self.csv_path.exists():
            return set()
        try:
            df = pd.read_csv(self.csv_path, usecols=['species_key'])
            return set(df['species_key'].astype(int).tolist())
        except Exception:
            return set()

    def _ensure_csv_header(self):
        if not self.csv_path.exists():
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(['species_key', 'url', 'media_type', 'source_name', 'source_url', 'license', 'accessed_date'])

    def append_records(self, records: List[Dict]):
        if not records:
            return
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            for r in records:
                writer.writerow([
                    r['species_key'], r['url'], r['media_type'],
                    r['source_name'], r['source_url'], r['license'], r['accessed_date']
                ])

    # ---------- GBIF name map ----------

    def load_gbif_name_map(self) -> Dict[str, int]:
        """{nombre_lower: species_key} usando canonical_name (+ scientific fallback)."""
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

    # ---------- Descarga ----------

    def fetch_page(self, group: str, page: int) -> Optional[Dict]:
        """Pide una pagina de un grupo. Devuelve el JSON o None."""
        params = {"query": f'grp:"{group}"', "key": self.api_key, "page": page}
        for attempt in range(4):
            try:
                resp = self.session.get(XENO_CANTO_API, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                wait = 5 * (2 ** attempt)
                logger.warning(f"Intento {attempt+1}/4 fallo ({group} p{page}): {e}")
                if attempt < 3:
                    time.sleep(wait)
                else:
                    logger.error(f"Pagina fallo ({group} p{page})")
                    return None

    def download_sounds(self, name_map: Dict[str, int]):
        logger.info("=" * 60)
        logger.info("DESCARGANDO SONIDOS DE XENO-CANTO (todos los grupos)")
        logger.info("=" * 60)

        accessed = date.today().isoformat()
        self._ensure_csv_header()

        progress = self.load_progress()
        seen = self.load_seen_species()
        logger.info(f"Especies ya guardadas: {len(seen)}")

        try:
            for group in GROUPS:
                start_page = progress.get(group, 0) + 1
                logger.info(f"\n--- Grupo: {group} (desde pagina {start_page}) ---")

                # Primera pagina para conocer numPages
                first = self.fetch_page(group, start_page)
                if first is None:
                    continue
                try:
                    num_pages = int(first.get("numPages", 1))
                except (ValueError, TypeError):
                    num_pages = 1
                logger.info(f"  {group}: {num_pages} paginas")

                page = start_page
                data = first
                while page <= num_pages:
                    if data is None:
                        data = self.fetch_page(group, page)
                        if data is None:
                            break

                    recordings = data.get("recordings", []) or []
                    new_records = []
                    for rec in recordings:
                        gen = (rec.get('gen') or '').strip()
                        sp = (rec.get('sp') or '').strip()
                        if not gen or not sp:
                            continue
                        name = f"{gen} {sp}".lower()
                        skey = name_map.get(name)
                        if skey is None or skey in seen:
                            continue
                        file_url = (rec.get('file') or '').strip()
                        if not file_url:
                            continue
                        seen.add(skey)
                        new_records.append({
                            'species_key': skey,
                            'url': file_url,
                            'media_type': 'sound',
                            'source_name': 'xeno_canto',
                            'source_url': (rec.get('url') or '').strip(),
                            'license': (rec.get('lic') or '').strip(),
                            'accessed_date': accessed,
                        })

                    self.append_records(new_records)
                    progress[group] = page
                    self.save_progress(progress)

                    if page % 20 == 0 or page >= num_pages:
                        logger.info(f"  {group}: pagina {page}/{num_pages} | especies totales: {len(seen)}")

                    data = None
                    page += 1
                    time.sleep(1)  # Xeno-canto pide ~1 req/s

        except KeyboardInterrupt:
            logger.warning("Interrumpido. El progreso quedo guardado; vuelve a correr para retomar.")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)

        logger.info(f"\nTerminado. Especies con sonido: {len(seen)}")

    def generate_report(self):
        if not self.csv_path.exists():
            return
        try:
            df = pd.read_csv(self.csv_path)
        except Exception as e:
            logger.warning(f"No se pudo leer CSV para reporte: {e}")
            return
        logger.info("\n" + "=" * 60)
        logger.info("ESTADISTICAS ACUMULADAS")
        logger.info("=" * 60)
        logger.info(f"Total grabaciones (1 por especie): {len(df)}")
        if not df.empty:
            logger.info(f"Especies unicas: {df['species_key'].nunique()}")
        logger.info("=" * 60)


def fetch_xeno_canto_urls():
    """Funcion principal. Llamada por download_all.py"""
    logger.info(f"Inicio: {datetime.now()}")

    api_key = get_api_key()
    if not api_key:
        logger.error("Falta XENO_CANTO_API_KEY. Registrate en xeno-canto.org y define la variable.")
        return

    fetcher = XenoCantoFetcher(api_key=api_key)

    name_map = fetcher.load_gbif_name_map()
    if not name_map:
        logger.error("No se pudo cargar GBIF")
        return

    fetcher.download_sounds(name_map)
    fetcher.generate_report()

    logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    fetch_xeno_canto_urls()