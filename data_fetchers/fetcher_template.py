"""
Template para crear nuevos fetchers
Copia este archivo como base para crear eol_fetcher.py, fishbase_fetcher.py, etc.

INSTRUCCIONES:
1. Copiar este archivo a data_fetchers/SOURCE_NAME_fetcher.py
2. Reemplazar SOURCE_NAME por el nombre de la fuente (eol, fishbase, etc)
3. Implementar los métodos según la fuente de datos
4. Agregar a download_all.py en AVAILABLE_FETCHERS
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime

# ==================== CONFIGURACIÓN ====================

OUTPUT_DIR = Path("biodiversity_data/SOURCE_NAME")  # ← CAMBIAR SOURCE_NAME
LOG_FILE = OUTPUT_DIR / "source_name_fetcher.log"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SourceNameFetcher:
    """
    Descarga datos de [FUENTE].
    
    Replace:
    - SourceName con el nombre de la clase (EOLDescriptionsFetcher, FishBaseFetcher, etc)
    - OUTPUT_DIR arriba
    - Los métodos según tu fuente de datos
    """
    
    def __init__(self, output_dir: str = "biodiversity_data/source_name"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output dir: {self.output_dir}")
    
    def fetch_raw_data(self) -> Optional[pd.DataFrame]:
        """
        Descarga datos crudos de la fuente.
        
        Implementar según tu API/fuente:
        - API requests
        - File downloads
        - Database queries
        - Web scraping (si es necesario)
        
        Returns:
            DataFrame con datos crudos
        """
        
        logger.info("Descargando datos crudos...")
        
        try:
            # TODO: Implementar descarga
            # Ejemplo:
            # response = requests.get("https://api.example.com/data")
            # data = response.json()
            # df = pd.DataFrame(data)
            
            logger.info("✓ Descarga completada")
            return df  # Cambiar por tu variable
            
        except Exception as e:
            logger.error(f"Error descargando: {e}")
            return None
    
    def parse_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parsea y normaliza datos.
        
        Args:
            df: DataFrame con datos crudos
        
        Returns:
            DataFrame limpio y normalizado
        """
        
        logger.info("Parseando datos...")
        
        try:
            # TODO: Implementar parsing
            # Ejemplo:
            # df = df[['columna1', 'columna2', 'columna3']]
            # df.columns = ['col1_renamed', 'col2_renamed', 'col3_renamed']
            # df = df.dropna(subset=['col1_renamed'])
            # df['col1_renamed'] = df['col1_renamed'].str.lower().str.strip()
            
            logger.info(f"✓ Parseado: {len(df)} registros")
            return df
            
        except Exception as e:
            logger.error(f"Error parseando: {e}")
            return None
    
    def match_with_gbif(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Matchea datos con species_key de GBIF.
        
        Args:
            df: DataFrame con datos parseados
        
        Returns:
            DataFrame con species_key agregado
        """
        
        logger.info("Matcheando con GBIF...")
        
        try:
            # 1. Cargar species_keys de GBIF
            gbif_file = Path("biodiversity_data/gbif_taxonomy.csv")
            
            if not gbif_file.exists():
                logger.error(f"No se encontró: {gbif_file}")
                return df
            
            gbif_df = pd.read_csv(
                gbif_file,
                usecols=['species_key', 'scientific_name']
            )
            
            # 2. Normalizar nombres
            gbif_df['scientific_name_lower'] = (
                gbif_df['scientific_name']
                .str.lower()
                .str.strip()
            )
            
            # 3. Merge/Join por nombre científico
            # TODO: Ajustar según tu columna de nombre científico
            df_matched = df.merge(
                gbif_df[['species_key', 'scientific_name_lower']],
                left_on='scientific_name',  # ← CAMBIAR si tu columna tiene otro nombre
                right_on='scientific_name_lower',
                how='left'
            )
            
            matched_count = df_matched['species_key'].notna().sum()
            logger.info(f"✓ Matched: {matched_count}/{len(df)}")
            
            return df_matched
            
        except Exception as e:
            logger.error(f"Error en matching: {e}")
            return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = "data.csv") -> Optional[Path]:
        """
        Guarda DataFrame a CSV listo para Neo4j.
        
        Args:
            df: DataFrame a guardar
            filename: Nombre del archivo
        
        Returns:
            Path del archivo guardado
        """
        
        logger.info(f"Guardando a CSV...")
        
        try:
            output_file = self.output_dir / filename
            
            # Asegurar que tenemos species_key
            if 'species_key' not in df.columns:
                logger.warning("DataFrame no tiene 'species_key'!")
            
            # Limpiar antes de guardar
            df = df.dropna(subset=['species_key'], how='all')
            
            df.to_csv(output_file, index=False)
            
            logger.info(f"✓ Guardado: {output_file}")
            logger.info(f"  Registros: {len(df)}")
            logger.info(f"  Tamaño: {output_file.stat().st_size / 1024 / 1024:.2f} MB")
            
            return output_file
            
        except Exception as e:
            logger.error(f"Error guardando: {e}")
            return None
    
    def generate_report(self, df: pd.DataFrame):
        """Genera reporte de estadísticas."""
        
        logger.info("\n" + "=" * 60)
        logger.info("ESTADÍSTICAS")
        logger.info("=" * 60)
        
        logger.info(f"Total registros: {len(df)}")
        
        # Mostrar columnas
        logger.info(f"\nColumnas ({len(df.columns)}):")
        for col in df.columns:
            non_null = df[col].notna().sum()
            null_pct = (df[col].isna().sum() / len(df) * 100)
            logger.info(f"  {col}: {non_null} ({null_pct:.1f}% null)")
        
        # Información específica según fuentes
        if 'species_key' in df.columns:
            unique_species = df['species_key'].nunique()
            logger.info(f"\nEspecies únicas: {unique_species}")
        
        logger.info("=" * 60)


def fetch_source_name():
    """
    Función principal que ejecuta todo el flujo.
    
    IMPORTANTE: El nombre de esta función debe estar en
    download_all.py en AVAILABLE_FETCHERS bajo "function"
    """
    
    logger.info("=" * 60)
    logger.info("INICIANDO DESCARGA: [SOURCE_NAME]")  # ← CAMBIAR
    logger.info("=" * 60)
    logger.info(f"Inicio: {datetime.now()}\n")
    
    # Crear fetcher
    fetcher = SourceNameFetcher()  # ← CAMBIAR clase
    
    try:
        # 1. Descargar
        raw_df = fetcher.fetch_raw_data()
        if raw_df is None or raw_df.empty:
            logger.error("No se obtuvieron datos crudos")
            return
        
        # 2. Parsear
        parsed_df = fetcher.parse_data(raw_df)
        if parsed_df is None or parsed_df.empty:
            logger.error("DataFrame vacío después de parsing")
            return
        
        # 3. Matchear con GBIF
        matched_df = fetcher.match_with_gbif(parsed_df)
        
        # 4. Guardar a CSV
        output_file = fetcher.save_to_csv(matched_df, "source_name.csv")  # ← CAMBIAR filename
        
        # 5. Generar reporte
        fetcher.generate_report(matched_df)
        
        logger.info(f"\n✓ Descarga completada exitosamente")
        
    except Exception as e:
        logger.error(f"Error fatal: {e}", exc_info=True)
    
    finally:
        logger.info(f"Fin: {datetime.now()}")


if __name__ == "__main__":
    fetch_source_name()