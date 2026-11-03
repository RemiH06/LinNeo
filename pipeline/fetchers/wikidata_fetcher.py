"""
Wikidata Common Names Fetcher
Descarga nombres comunes en múltiples idiomas desde Wikidata.

Descarga de: https://query.wikidata.org/sparql
Guardado en: biodiversity_data/wikidata/wikidata_common_names.csv

Estructura similar a download_biodiversity_data.py para consistencia.
"""

import requests
import pandas as pd
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional
from difflib import SequenceMatcher
from datetime import datetime

# ==================== CONFIGURACIÓN ====================

LANGUAGES = ["en", "es", "fr", "de", "pt", "ja", "zh", "it", "ru", "nl"]
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
OUTPUT_DIR = Path("biodiversity_data/wikidata")
LOG_FILE = OUTPUT_DIR / "wikidata_fetcher.log"

# Crear directorio antes de configurar logging
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WikidataCommonNamesFetcher:
    """Descarga y procesa nombres comunes desde Wikidata"""
    
    def __init__(self, output_dir: str = "biodiversity_data/wikidata"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sparql_endpoint = WIKIDATA_SPARQL
        self.languages = LANGUAGES
    
    def build_sparql_query(self, language: str, offset: int = 0, limit: int = 5000) -> str:
        """
        Construye query SPARQL para nombres comunes de UN idioma.
        Usa P1843 (taxon common name). Paginacion por OFFSET
        (ligero porque P1843 es un conjunto pequeno por idioma).
        """
        
        query = f"""
        SELECT ?item ?scientific_name ?commonName WHERE {{
          ?item wdt:P225 ?scientific_name .
          ?item wdt:P1843 ?commonName .
          FILTER(LANG(?commonName) = "{language}")
        }}
        ORDER BY ?item
        LIMIT {limit}
        OFFSET {offset}
        """
        
        return query
    
    def query_wikidata(self, query: str, retry: int = 4) -> Optional[List[Dict]]:
        """Ejecuta query SPARQL con reintentos y backoff exponencial."""
        
        headers = {"User-Agent": "LinNeo/1.0 (https://github.com/RemiH06/LinNeo)"}
        params = {"query": query, "format": "json"}
        
        for attempt in range(retry):
            try:
                response = requests.get(
                    self.sparql_endpoint,
                    params=params,
                    headers=headers,
                    timeout=180
                )
                response.raise_for_status()
                
                data = response.json()
                results = data.get("results", {}).get("bindings", [])
                
                logger.info(f"Query exitosa: {len(results)} resultados")
                return results
                
            except requests.exceptions.RequestException as e:
                wait = 10 * (2 ** attempt)  # 10, 20, 40, 80 segundos
                logger.warning(f"Intento {attempt + 1}/{retry} fallo: {e}")
                if attempt < retry - 1:
                    logger.info(f"Esperando {wait}s antes de reintentar...")
                    time.sleep(wait)
                else:
                    logger.error(f"Query fallo despues de {retry} intentos")
                    return None
    
    def parse_wikidata_result(self, result: Dict, language: str) -> Optional[Dict]:
        """Parsea resultado de Wikidata. El idioma se pasa aparte (no viene en la respuesta)."""
        
        try:
            item_url = result.get("item", {}).get("value", "")
            wikidata_id = item_url.split("/")[-1] if item_url else None
            
            scientific_name = result.get("scientific_name", {}).get("value", "").lower().strip()
            common_name = result.get("commonName", {}).get("value", "").strip()
            
            if not all([wikidata_id, scientific_name, common_name]):
                return None
            
            return {
                "wikidata_id": wikidata_id,
                "scientific_name": scientific_name,
                "common_name": common_name,
                "language": language
            }
        except Exception as e:
            logger.warning(f"Error parseando resultado: {e}")
            return None
    
    def download_common_names(self, batch_size: int = 5000, max_per_language: int = 500000) -> pd.DataFrame:
        """
        Descarga nombres comunes de Wikidata, idioma por idioma,
        usando paginación por cursor (eficiente, sin timeouts).
        
        Args:
            batch_size: Registros por request (max 10000 recomendado)
            max_per_language: Tope de registros por idioma (seguridad)
        
        Returns:
            DataFrame con nombres comunes
        """
        
        logger.info("=" * 60)
        logger.info("DESCARGANDO NOMBRES COMUNES DE WIKIDATA")
        logger.info("=" * 60)
        
        all_data = []
        
        try:
            for language in self.languages:
                logger.info(f"\n--- Idioma: {language} ---")
                offset = 0
                lang_count = 0
                
                while lang_count < max_per_language:
                    query = self.build_sparql_query(
                        language=language,
                        offset=offset,
                        limit=batch_size
                    )
                    results = self.query_wikidata(query)
                    
                    if results is None or len(results) == 0:
                        logger.info(f"  '{language}' completado: {lang_count} nombres")
                        break
                    
                    for result in results:
                        parsed = self.parse_wikidata_result(result, language)
                        if parsed:
                            all_data.append(parsed)
                    
                    lang_count += len(results)
                    offset += batch_size
                    logger.info(f"  '{language}': {lang_count} (total global: {len(all_data)})")
                    
                    # Si el batch vino incompleto, no hay mas
                    if len(results) < batch_size:
                        logger.info(f"  '{language}' completado: {lang_count} nombres")
                        break
                    
                    time.sleep(2)  # Rate limiting respetuoso (evita 429)
        
        except KeyboardInterrupt:
            logger.warning("Descarga interrumpida por usuario")
        except Exception as e:
            logger.error(f"Error durante descarga: {e}", exc_info=True)
        
        logger.info(f"\nDescarga completada: {len(all_data)} registros")
        return pd.DataFrame(all_data)
    
    def match_with_gbif(self, wikidata_df: pd.DataFrame, gbif_scientific_names: Dict[str, str]) -> pd.DataFrame:
        """
        Matchea nombres de Wikidata con species_key de GBIF.
        
        Args:
            wikidata_df: DataFrame con datos de Wikidata
            gbif_scientific_names: {scientific_name: species_key}
        
        Returns:
            DataFrame con matches
        """
        
        logger.info("Matcheando Wikidata con GBIF...")
        
        matched_records = []
        
        for _, row in wikidata_df.iterrows():
            wikidata_sci_name = row['scientific_name'].lower().strip()
            
            # Exact match
            if wikidata_sci_name in gbif_scientific_names:
                species_key = gbif_scientific_names[wikidata_sci_name]
                matched_records.append({
                    'species_key': species_key,
                    'common_name': row['common_name'],
                    'language': row['language'],
                    'wikidata_id': row['wikidata_id'],
                    'match_type': 'exact'
                })
        
        logger.info(f"✓ Matched: {len(matched_records)} registros")
        return pd.DataFrame(matched_records)
    
    def load_gbif_species_names(self) -> Dict[str, str]:
        """
        Carga nombres de GBIF desde CSV para matching.
        Usa canonical_name (sin autor) que coincide con P225 de Wikidata,
        y tambien scientific_name como fallback.
        
        Returns:
            {nombre_lower: species_key}
        """
        
        logger.info("Cargando species_keys de GBIF...")
        
        gbif_file = Path("biodiversity_data/gbif_taxonomy.csv")
        
        if not gbif_file.exists():
            logger.error(f"No se encontro: {gbif_file}")
            return {}
        
        try:
            df = pd.read_csv(gbif_file, usecols=['species_key', 'scientific_name', 'canonical_name'])
            
            species_map = {}
            for key, sci, canon in zip(df['species_key'], df['scientific_name'], df['canonical_name']):
                skey = str(key)
                # canonical_name primero (coincide con Wikidata P225)
                if pd.notna(canon):
                    species_map[canon.lower().strip()] = skey
                # scientific_name como fallback (no sobreescribe canonical)
                if pd.notna(sci):
                    species_map.setdefault(sci.lower().strip(), skey)
            
            logger.info(f"Cargados {len(species_map)} nombres de GBIF")
            return species_map
        except Exception as e:
            logger.error(f"Error cargando GBIF: {e}")
            return {}
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = "wikidata_common_names.csv"):
        """Guarda DataFrame a CSV."""
        
        output_file = self.output_dir / filename
        
        if df.empty:
            logger.warning("DataFrame vacío, no se guardó archivo")
            return None
        
        try:
            df.to_csv(output_file, index=False)
            logger.info(f"✓ Guardado: {output_file} ({len(df)} registros)")
            return output_file
        except Exception as e:
            logger.error(f"Error guardando archivo: {e}")
            return None
    
    def generate_report(self, df: pd.DataFrame):
        """Genera reporte de estadísticas."""
        
        logger.info("\n" + "=" * 60)
        logger.info("ESTADÍSTICAS FINALES")
        logger.info("=" * 60)
        
        logger.info(f"\nTotal registros: {len(df)}")
        
        if not df.empty:
            logger.info(f"\nPor idioma:")
            lang_counts = df['language'].value_counts()
            for lang, count in lang_counts.items():
                logger.info(f"  {lang}: {count}")
            
            unique_species = df['species_key'].nunique()
            logger.info(f"\nEspecies únicas: {unique_species}")
            
            match_types = df['match_type'].value_counts()
            logger.info(f"\nPor tipo de match:")
            for mt, count in match_types.items():
                logger.info(f"  {mt}: {count}")
        
        logger.info("=" * 60)


def fetch_wikidata_common_names(use_gbif_matching: bool = True):
    """
    Ejecuta el fetcher completo de Wikidata.
    
    Args:
        use_gbif_matching: Si True, matchea con species_key de GBIF
    """
    
    fetcher = WikidataCommonNamesFetcher()
    
    # 1. Descargar de Wikidata
    wikidata_df = fetcher.download_common_names()
    
    if wikidata_df.empty:
        logger.error("No se descargaron datos de Wikidata")
        return
    
    # 2. Matchear con GBIF si está habilitado
    if use_gbif_matching:
        gbif_species = fetcher.load_gbif_species_names()
        
        if gbif_species:
            matched_df = fetcher.match_with_gbif(wikidata_df, gbif_species)
            
            # Guardar CSV matched
            fetcher.save_to_csv(matched_df, "wikidata_common_names.csv")
            
            # Generar reporte
            fetcher.generate_report(matched_df)
        else:
            logger.warning("No se pudo hacer matching con GBIF")
            # Guardar sin matching
            fetcher.save_to_csv(wikidata_df, "wikidata_common_names_raw.csv")
    else:
        # Guardar sin matching
        fetcher.save_to_csv(wikidata_df, "wikidata_common_names_raw.csv")
    
    logger.info(f"\nFin: {datetime.now()}")


if __name__ == "__main__":
    logger.info(f"Inicio: {datetime.now()}")
    fetch_wikidata_common_names()