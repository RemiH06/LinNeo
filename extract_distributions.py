#!/usr/bin/env python3
"""
Script para extraer distribuciones geográficas del GBIF Backbone
Usa el archivo Distribution.tsv que ya descargaste
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def read_distributions_line_by_line(dist_file: Path):
    """
    Método fallback: leer el archivo línea por línea
    Más lento pero más robusto para archivos problemáticos
    """
    logger.info("Leyendo archivo línea por línea (esto puede tardar)...")
    
    distributions = []
    headers = None
    taxon_idx = None
    country_idx = None
    processed = 0
    errors = 0
    
    with open(dist_file, 'r', encoding='utf-8', errors='ignore') as f:
        for i, line in enumerate(f):
            try:
                if i == 0:
                    # Header
                    headers = line.strip().split('\t')
                    try:
                        taxon_idx = headers.index('taxonID')
                        country_idx = headers.index('countryCode')
                    except ValueError:
                        logger.error("No se encontraron columnas taxonID o countryCode")
                        return []
                    continue
                
                fields = line.strip().split('\t')
                
                if len(fields) > max(taxon_idx, country_idx):
                    taxon_id = fields[taxon_idx]
                    country_code = fields[country_idx]
                    
                    if taxon_id and country_code:
                        distributions.append({
                            'taxonID': taxon_id,
                            'countryCode': country_code
                        })
                        processed += 1
                        
                        if processed % 100000 == 0:
                            logger.info(f"Procesadas {processed:,} líneas (errores: {errors})...")
                
            except Exception as e:
                errors += 1
                if errors <= 10:
                    logger.debug(f"Error en línea {i}: {e}")
                continue
    
    logger.info(f"Procesamiento línea por línea completado: {processed:,} distribuciones")
    
    if distributions:
        return [pd.DataFrame(distributions)]
    return []


def extract_distributions(
    backbone_dir: str = "biodiversity_data/backbone_extract",
    output_dir: str = "biodiversity_data"
):
    """
    Extrae distribuciones del archivo Distribution.tsv del Backbone
    """
    backbone_path = Path(backbone_dir)
    output_path = Path(output_dir)
    
    logger.info("="*60)
    logger.info("EXTRAYENDO DISTRIBUCIONES GEOGRÁFICAS")
    logger.info("="*60)
    
    # Verificar que existe Distribution.tsv
    dist_file = backbone_path / "Distribution.tsv"
    if not dist_file.exists():
        logger.error(f"No se encontró {dist_file}")
        logger.error("Asegúrate de haber descargado el backbone completo")
        return
    
    logger.info(f"Leyendo {dist_file}...")
    logger.info("Este archivo puede ser grande, procesando por chunks...")
    
    # Leer por chunks
    chunk_size = 100000
    all_distributions = []
    total_read = 0
    
    # Intentar con diferentes métodos de parsing
    methods = [
        {
            'name': 'C engine con warn',
            'params': {
                'sep': '\t',
                'chunksize': chunk_size,
                'encoding': 'utf-8',
                'on_bad_lines': 'warn',
                'engine': 'c'
            }
        },
        {
            'name': 'Python engine con skip',
            'params': {
                'sep': '\t',
                'chunksize': chunk_size,
                'encoding': 'utf-8',
                'on_bad_lines': 'skip',
                'engine': 'python',
                'quoting': 3  # QUOTE_NONE
            }
        }
    ]
    
    for method in methods:
        logger.info(f"Intentando: {method['name']}...")
        all_distributions = []
        total_read = 0
        
        try:
            for chunk in pd.read_csv(dist_file, **method['params']):
                # Filtrar columnas relevantes
                if 'taxonID' in chunk.columns and 'countryCode' in chunk.columns:
                    distributions = chunk[['taxonID', 'countryCode']].copy()
                    
                    # Intentar agregar columnas opcionales si existen
                    if 'locality' in chunk.columns:
                        distributions['locality'] = chunk['locality']
                    if 'locationID' in chunk.columns:
                        distributions['locationID'] = chunk['locationID']
                    
                    distributions = distributions.dropna(subset=['taxonID', 'countryCode'])
                    all_distributions.append(distributions)
                    total_read += len(distributions)
                    
                    if total_read % 100000 == 0:
                        logger.info(f"Procesadas {total_read:,} distribuciones...")
            
            # Si llegamos aquí, fue exitoso
            logger.info(f"✓ {method['name']} exitoso")
            break
            
        except Exception as e:
            logger.warning(f"✗ {method['name']} falló: {e}")
            continue
    
    if not all_distributions:
        logger.warning("No se encontraron distribuciones válidas con ningún método")
        logger.info("\nIntentando método alternativo: leer línea por línea...")
        all_distributions = read_distributions_line_by_line(dist_file)
        
        if not all_distributions:
            logger.error("No se pudo procesar el archivo Distribution.tsv")
            return
    
    # Combinar todos los chunks
    logger.info("Combinando datos...")
    df = pd.concat(all_distributions, ignore_index=True)
    
    # Cargar taxonomía para obtener nombres de especies
    logger.info("Cargando taxonomía...")
    taxonomy_file = output_path / "gbif_taxonomy.csv"
    
    if taxonomy_file.exists():
        taxonomy_df = pd.read_csv(taxonomy_file)
        
        # Hacer join para obtener nombres
        df = df.merge(
            taxonomy_df[['species_key', 'scientific_name', 'kingdom', 'phylum', 'class']],
            left_on='taxonID',
            right_on='species_key',
            how='left'
        )
    
    # Renombrar columnas
    df = df.rename(columns={
        'taxonID': 'species_key',
        'countryCode': 'country_code'
    })
    
    # Eliminar duplicados (una especie puede estar múltiples veces en un país)
    df = df.drop_duplicates(subset=['species_key', 'country_code'])
    
    # Guardar
    output_file = output_path / "species_geographic_relationships.csv"
    df.to_csv(output_file, index=False)
    
    logger.info("="*60)
    logger.info(f"Distribuciones extraídas: {len(df):,}")
    
    try:
        species_unique = df['species_key'].nunique()
        country_unique = df['country_code'].nunique()
        
        logger.info(f"Especies únicas con distribución: {species_unique:,}")
        logger.info(f"Países únicos: {country_unique:,}")
    except Exception as e:
        logger.warning(f"No se pudieron calcular estadísticas: {e}")
    
    logger.info(f"Guardado en: {output_file}")
    logger.info("="*60)
    
    # Estadísticas
    logger.info("\nEstadísticas por reino:")
    if 'kingdom' in df.columns:
        stats = df.groupby('kingdom')['species_key'].nunique().sort_values(ascending=False)
        for kingdom, count in stats.items():
            logger.info(f"  {kingdom}: {count:,} especies")
    
    return df


def main():
    extract_distributions()
    
    logger.info("\nPróximos pasos:")
    logger.info("1. Si usas neo4j-admin: ejecuta prepare_neo4j_import.py de nuevo")
    logger.info("2. Si usas Python import: ejecuta import_to_neo4j.py --clear")


if __name__ == "__main__":
    main()