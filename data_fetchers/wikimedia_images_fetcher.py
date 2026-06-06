"""
Wikimedia / Wikipedia Images Fetcher
Descarga URLs de imagen principal de cada especie via la API de Wikipedia
(prop=pageimages). Mismo patron de batching por titulos que las descripciones
(rapido y confiable), en vez de SPARQL de Wikidata (que hace timeout en bulk).

Caracteristicas:
- Checkpoint/resume: guarda progreso y retoma donde quedo
- Escritura incremental al CSV
- Batchea hasta 50 titulos por request; sigue redirects
- Intenta idiomas en orden (en, luego es)

Salida: biodiversity_data/images/wikimedia_images.csv
Progreso: biodiversity_data/images/wikimedia_images_progress.txt
Columnas: species_key, url, media_type, source_name, source_url, license, accessed_date

Modelo destino (opcion B):
  (Species)-[:HAS_MEDIA]->(:Media {media_type:"image", url, source_name, source_url, ...})
"""

import requests
import pandas as pd
import time
import logging
import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime, date
from urllib.parse import quote

# ==================== CONFIGURACION ====================

LANGUAGES = ["en", "es"]
WIKIPEDIA_API = "https://{lang}.wikipedia.org/w/api.php"

# Filtrar por reino (None = todos). Ej: ["Animalia", "Plantae", "Fungi"]
KINGDOM_FILTER = None

OUTPUT_DIR = Path("biodiversity_data/images")
OUTPUT_FILE = OUTPUT_DIR / "wikimedia_images.csv"
PROGRESS_FILE = OUTPUT_DIR / "wikimedia_images_progress.txt"
LOG_FILE = OUTPUT_DIR / "wikimedia_images_fetcher.log"

# pageimages permite hasta 50 titulos por request
BATCH_TITLES = 50
FLUSH_EVERY = 5

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


class WikimediaImagesFetcher:
    """Descarga URLs de imagen principal via Wikipedia pageimages, con resume."""

    def __init__(self, output_dir: str = "biodiversity_data/images"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "wikimedia_images.csv"
        self.progress_path = self.output_dir / "wikimedia_images_progress.txt"
        self.languages = LANGUAGES
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
                processed = {int(line.strip()) for line in f if line.strip()}
            logger.info(f"Checkpoint: {len(processed)} especies ya procesadas, se omitiran")
            return processed
        except Exception as e:
            logger.warning(f"No se pudo leer checkpoint: {e}")
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

    def mark_processed(self, keys: List[int]):
        if not keys:
            return
        with open(self.progress_path, 'a', encoding='utf-8') as f:
            for k in keys:
                f.write(f"{k}\n")

    # ---------- Carga de especies ----------

    def load_gbif_species(self, kingdom_filter: Optional[List[str]] = None) -> pd.DataFrame:
        logger.info("Cargando especies de GBIF...")
        gbif_file = Path("biodiversity_data/gbif_taxonomy.csv")
        if not gbif_file.exists():
            logger.error(f"No se encontro: {gbif_file}")
            return pd.DataFrame()
        try:
            df = pd.read_csv(
                gbif_file,
                usecols=['species_key', 'canonical_name', 'kingdom'],
                low_memory=False
            )
            df = df.dropna(subset=['species_key', 'canonical_name'])
            if kingdom_filter:
                df = df[df['kingdom'].isin(kingdom_filter)]
                logger.info(f"Filtrado a reinos {kingdom_filter}")
            df = df.drop_duplicates(subset=['canonical_name'])
            df['species_key'] = df['species_key'].astype(int)
            logger.info(f"Cargadas {len(df)} especies")
            return df[['species_key', 'canonical_name']].reset_index(drop=True)
        except Exception as e:
            logger.error(f"Error cargando GBIF: {e}")
            return pd.DataFrame()

    # ---------- Descarga ----------

    def fetch_batch(self, titles: List[str], lang: str) -> Dict[str, str]:
        """
        Pide imagen principal de un lote de titulos. Devuelve {canonical_name: image_url}.
        """
        url = WIKIPEDIA_API.format(lang=lang)
        params = {
            "action": "query",
            "format": "json",
            "prop": "pageimages",
            "piprop": "original",
            "pilimit": "max",
            "redirects": 1,
            "titles": "|".join(titles),
        }

        data = None
        for attempt in range(4):
            try:
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                break
            except (requests.exceptions.RequestException, ValueError) as e:
                wait = 5 * (2 ** attempt)
                logger.warning(f"Intento {attempt+1}/4 fallo ({lang}): {e}")
                if attempt < 3:
                    time.sleep(wait)
                else:
                    logger.error(f"Lote fallo despues de 4 intentos ({lang})")
                    return {}

        title_map = {t: t for t in titles}
        norm = {n['to']: n['from'] for n in data.get('query', {}).get('normalized', [])}
        redir = {r['to']: r['from'] for r in data.get('query', {}).get('redirects', [])}

        def resolve_original(final_title: str) -> Optional[str]:
            current = final_title
            for _ in range(5):
                if current in title_map:
                    return current
                if current in redir:
                    current = redir[current]
                    continue
                if current in norm:
                    current = norm[current]
                    continue
                return None
            return None

        results = {}
        pages = data.get('query', {}).get('pages', {})
        for page in pages.values():
            if 'missing' in page:
                continue
            original = page.get('original')
            if not original or not original.get('source'):
                continue
            name = resolve_original(page.get('title', ''))
            if name:
                results[name] = original['source']
        return results

    def download_images(self, species_df: pd.DataFrame):
        logger.info("=" * 60)
        logger.info("DESCARGANDO IMAGENES VIA WIKIPEDIA (pageimages)")
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
            logger.info("Nada pendiente. Todo procesado.")
            return

        key_map = dict(zip(pending['canonical_name'], pending['species_key']))
        names = list(pending['canonical_name'])

        found_count = 0
        done_count = 0
        batch_num = 0

        try:
            for i in range(0, len(names), BATCH_TITLES):
                batch = names[i:i + BATCH_TITLES]
                batch_records = []
                remaining = list(batch)

                for lang in self.languages:
                    if not remaining:
                        break
                    found = self.fetch_batch(remaining, lang)
                    next_remaining = []
                    for name in remaining:
                        if name in found:
                            skey = int(key_map[name])
                            article = f"https://{lang}.wikipedia.org/wiki/{quote(name.replace(' ', '_'))}"
                            batch_records.append({
                                'species_key': skey,
                                'url': found[name],
                                'media_type': 'image',
                                'source_name': 'wikimedia_commons',
                                'source_url': article,
                                'license': '',
                                'accessed_date': accessed,
                            })
                        else:
                            next_remaining.append(name)
                    remaining = next_remaining
                    time.sleep(0.3)

                self.append_records(batch_records)
                self.mark_processed([int(key_map[n]) for n in batch])

                found_count += len(batch_records)
                done_count += len(batch)
                batch_num += 1

                if batch_num % FLUSH_EVERY == 0 or done_count >= total:
                    logger.info(f"  Progreso: {done_count}/{total} | con imagen: {found_count}")

        except KeyboardInterrupt:
            logger.warning("Interrumpido. El progreso quedo guardado; vuelve a correr para retomar.")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)

        logger.info(f"\nLote terminado. Con imagen en esta corrida: {found_count}/{total}")

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
        logger.info(f"Total imagenes: {len(df)}")
        if not df.empty:
            logger.info(f"Especies unicas: {df['species_key'].nunique()}")
        logger.info("=" * 60)


def fetch_wikimedia_images(kingdom_filter: Optional[List[str]] = KINGDOM_FILTER):
    """Funcion principal. Llamada por download_all.py"""
    logger.info(f"Inicio: {datetime.now()}")

    fetcher = WikimediaImagesFetcher()

    species_df = fetcher.load_gbif_species(kingdom_filter=kingdom_filter)
    if species_df.empty:
        logger.error("No hay especies para procesar")
        return

    fetcher.download_images(species_df)
    fetcher.generate_report()

    logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    fetch_wikimedia_images()