"""
Wikipedia Descriptions Fetcher
Descarga descripciones (primeros 2 parrafos) de especies desde Wikipedia.

Caracteristicas:
- Checkpoint/resume: guarda progreso y retoma donde quedo
- Escritura incremental al CSV (no pierde lo descargado si se interrumpe)
- Solo primeros 2 parrafos (usa exintro para traer solo la intro)
- Batchea hasta 20 titulos por request; sigue redirects
- Intenta idiomas en orden (en, luego es) dentro del mismo lote

Salida: biodiversity_data/descriptions/wikipedia_descriptions.csv
Progreso: biodiversity_data/descriptions/wikipedia_progress.txt
Columnas: species_key, text, lang, source_name, source_url, accessed_date

Estructura consistente con wikidata_fetcher.py
"""

import duckdb
import requests
import pandas as pd
import time
import logging
import csv
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime, date
from urllib.parse import quote

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import DATA, ENRICHED, SYNONYMS, TAXA, WIKIDATA_DIR  # noqa: E402
from match import unique_names  # noqa: E402

# ==================== CONFIGURACION ====================

# Idiomas en orden de preferencia: intenta el primero, si no hay, el siguiente
LANGUAGES = ["en", "es"]

WIKIPEDIA_API = "https://{lang}.wikipedia.org/w/api.php"

# Filtrar por reino (None = todos). Ej: ["Animalia", "Plantae", "Fungi"]
KINGDOM_FILTER = None

# Cuantos parrafos guardar
MAX_PARAGRAPHS = 2

OUTPUT_DIR = DATA / "descriptions"
OUTPUT_FILE = OUTPUT_DIR / "wikipedia_descriptions.csv"
PROGRESS_FILE = OUTPUT_DIR / "wikipedia_progress.txt"
LOG_FILE = OUTPUT_DIR / "wikipedia_fetcher.log"

# La API de extracts permite max 20 titulos por request
BATCH_TITLES = 20

# Cada cuantos lotes loguear progreso
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


def first_n_paragraphs(text: str, n: int = MAX_PARAGRAPHS) -> str:
    """Devuelve los primeros n parrafos no vacios del texto."""
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    return "\n\n".join(paragraphs[:n])


def missing_titles_keys(lang: str) -> List[tuple]:
    """(título, taxonID) enlazados por Wikidata a un solo nodo sin texto en lang."""
    gbif_links = (WIKIDATA_DIR / f"gbif_{lang}.tsv").as_posix()
    return duckdb.sql(f"""
        WITH links AS (
            SELECT replace(url_decode(regexp_extract(title, '/wiki/(.*)>', 1)), '_', ' ') AS title,
                   TRY_CAST(trim(gbif, '"') AS BIGINT) AS gbif
            FROM read_csv('{gbif_links}', delim='\t', header=true, quote='', escape='',
                          names=['item', 'gbif', 'title'], all_varchar=true)
        ), nodes AS (
            SELECT taxonID AS id, taxonID FROM read_parquet('{TAXA.as_posix()}')
            UNION ALL
            SELECT taxonID, acceptedID FROM read_parquet('{SYNONYMS.as_posix()}')
        )
        SELECT l.title, min(n.taxonID)
        FROM links l JOIN nodes n ON l.gbif = n.id
        WHERE n.taxonID NOT IN (
            SELECT taxonID FROM read_parquet('{(ENRICHED / "wikipedia.parquet").as_posix()}')
            WHERE lang = '{lang}')
        GROUP BY l.title
        HAVING count(DISTINCT n.taxonID) = 1
        ORDER BY l.title
    """).fetchall()


class WikipediaDescriptionsFetcher:
    """Descarga descripciones (2 parrafos) desde Wikipedia con resume."""

    def __init__(self, output_dir: Path = DATA / "descriptions"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / "wikipedia_descriptions.csv"
        self.progress_path = self.output_dir / "wikipedia_progress.txt"
        self.languages = LANGUAGES
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LinNeo/1.0 (https://github.com/RemiH06/LinNeo)"
        })

    # ---------- Checkpoint ----------

    def load_processed(self) -> Set[int]:
        """Carga los species_key ya intentados (para retomar)."""
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
        """Escribe el header si el CSV aun no existe."""
        if not self.csv_path.exists():
            with open(self.csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerow(['species_key', 'text', 'lang', 'source_name', 'source_url', 'accessed_date'])

    def append_records(self, records: List[Dict]):
        """Agrega filas al CSV (modo append)."""
        if not records:
            return
        with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            for r in records:
                writer.writerow([
                    r['species_key'], r['text'], r['lang'],
                    r['source_name'], r['source_url'], r['accessed_date']
                ])

    def mark_processed(self, keys: List[int]):
        """Agrega species_key al archivo de progreso."""
        if not keys:
            return
        with open(self.progress_path, 'a', encoding='utf-8') as f:
            for k in keys:
                f.write(f"{k}\n")

    # ---------- Carga de especies ----------

    def load_gbif_species(self, kingdom_filter: Optional[List[str]] = None) -> pd.DataFrame:
        return unique_names(kingdoms=kingdom_filter)

    # ---------- Descarga ----------

    def fetch_batch(self, titles: List[str], lang: str) -> Dict[str, str]:
        """
        Pide un lote de titulos. Devuelve {canonical_name: texto_2_parrafos}.
        Usa exintro para traer solo la introduccion (mas rapido).
        """
        url = WIKIPEDIA_API.format(lang=lang)
        params = {
            "action": "query",
            "format": "json",
            "prop": "extracts",
            "explaintext": 1,
            "exintro": 1,
            "exlimit": "max",
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
            except requests.exceptions.RequestException as e:
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
            if 'missing' in page or page.get('pageid', 0) == 0:
                continue
            raw = page.get('extract', '').strip()
            if not raw or len(raw) < 50:
                continue
            text = first_n_paragraphs(raw)
            if len(text) < 50:
                continue
            original = resolve_original(page.get('title', ''))
            if original:
                results[original] = text

        return results

    def download_missing(self, lang: str, limit: Optional[int] = None):
        """Baja los artículos que Wikidata enlaza a un nodo que aún no tiene texto en lang.

        El título viene del enlace del item, no del nombre del taxón: así se alcanzan
        artículos como Bandfish, que por nombre nunca aparecieron. Lleva su propio
        progreso por título; un lote fallido no se marca y se reintenta en otra corrida.
        """
        progress = self.output_dir / f"wikipedia_missing_{lang}.txt"
        done = set(progress.read_text(encoding='utf-8').splitlines()) if progress.exists() else set()
        keys = dict(missing_titles_keys(lang))
        pending = [t for t in keys if t not in done][:limit]
        logger.info(f"Artículos pendientes en {lang}: {len(pending)}")

        accessed = date.today().isoformat()
        self._ensure_csv_header()
        for i in range(0, len(pending), BATCH_TITLES):
            batch = pending[i:i + BATCH_TITLES]
            found = self.fetch_batch(batch, lang)
            if not found:
                continue
            self.append_records([{
                'species_key': keys[title],
                'text': text,
                'lang': lang,
                'source_name': 'wikipedia',
                'source_url': f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}",
                'accessed_date': accessed,
            } for title, text in found.items()])
            with open(progress, 'a', encoding='utf-8') as f:
                f.write("".join(f"{title}\n" for title in batch))
            time.sleep(0.3)
            if (i // BATCH_TITLES) % FLUSH_EVERY == 0:
                logger.info(f"  Progreso: {i + len(batch)}/{len(pending)}")

    def download_descriptions(self, species_df: pd.DataFrame):
        """
        Descarga con resume y escritura incremental.
        Cada lote se resuelve completo (todos los idiomas) antes de marcarse.
        """
        logger.info("=" * 60)
        logger.info("DESCARGANDO DESCRIPCIONES DE WIKIPEDIA (2 parrafos)")
        logger.info("=" * 60)

        accessed = date.today().isoformat()
        self._ensure_csv_header()

        # Resume: quitar las ya procesadas
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

                # Intentar cada idioma sobre los que aun no se encontraron
                for lang in self.languages:
                    if not remaining:
                        break
                    found = self.fetch_batch(remaining, lang)
                    next_remaining = []
                    for name in remaining:
                        if name in found:
                            skey = int(key_map[name])
                            wiki_url = f"https://{lang}.wikipedia.org/wiki/{quote(name.replace(' ', '_'))}"
                            batch_records.append({
                                'species_key': skey,
                                'text': found[name],
                                'lang': lang,
                                'source_name': 'wikipedia',
                                'source_url': wiki_url,
                                'accessed_date': accessed,
                            })
                        else:
                            next_remaining.append(name)
                    remaining = next_remaining
                    time.sleep(0.3)

                # Persistir lote: descripciones + todos los keys intentados
                self.append_records(batch_records)
                self.mark_processed([int(key_map[n]) for n in batch])

                found_count += len(batch_records)
                done_count += len(batch)
                batch_num += 1

                if batch_num % FLUSH_EVERY == 0 or done_count >= total:
                    logger.info(f"  Progreso: {done_count}/{total} | con descripcion: {found_count}")

        except KeyboardInterrupt:
            logger.warning("Interrumpido. El progreso quedo guardado; vuelve a correr para retomar.")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)

        logger.info(f"\nLote terminado. Con descripcion en esta corrida: {found_count}/{total}")

    def generate_report(self):
        """Estadisticas leyendo el CSV acumulado."""
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
        logger.info(f"Total descripciones: {len(df)}")
        if not df.empty:
            logger.info("\nPor idioma:")
            for lang, count in df['lang'].value_counts().items():
                logger.info(f"  {lang}: {count}")
            avg_len = int(df['text'].astype(str).str.len().mean())
            logger.info(f"\nLongitud promedio: {avg_len} caracteres")
            logger.info(f"Especies unicas: {df['species_key'].nunique()}")
        logger.info("=" * 60)


def fetch_wikipedia_descriptions(kingdom_filter: Optional[List[str]] = KINGDOM_FILTER):
    """Funcion principal. Llamada por download_all.py"""
    logger.info(f"Inicio: {datetime.now()}")

    fetcher = WikipediaDescriptionsFetcher()

    species_df = fetcher.load_gbif_species(kingdom_filter=kingdom_filter)
    if species_df.empty:
        logger.error("No hay especies para procesar")
        return

    fetcher.download_descriptions(species_df)
    fetcher.generate_report()

    logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    # --missing <lang> [limite]: solo lo que falta, por título de Wikidata.
    if len(sys.argv) > 2 and sys.argv[1] == "--missing":
        limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
        WikipediaDescriptionsFetcher().download_missing(sys.argv[2], limit)
    else:
        fetch_wikipedia_descriptions()