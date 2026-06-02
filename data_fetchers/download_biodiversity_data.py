"""
Script para descargar datos taxonómicos y geográficos de biodiversidad
Descarga datos de GBIF y construye la jerarquía taxonómica y geográfica
"""

import requests
import pandas as pd
import json
import time
from pathlib import Path
from typing import Dict, List, Set
import zipfile
import io

class BiodiversityDataDownloader:
    """Descarga y procesa datos de biodiversidad y geografía"""
    
    def __init__(self, output_dir: str = "biodiversity_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.gbif_api = "https://api.gbif.org/v1"
        self.backbone_url = "https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip"
    
    def download_complete_backbone(self):
        """
        Descarga el GBIF Backbone Taxonomy completo (~2.8 millones de especies)
        
        Descarga el archivo oficial Darwin Core Archive y lo procesa.
        Tamaño: ~500 MB - 1 GB comprimido, ~2-3 GB descomprimido
        """
        import zipfile
        import shutil
        from io import BytesIO
        
        print("="*60)
        print("DESCARGANDO GBIF BACKBONE COMPLETO")
        print("="*60)
        print(f"\nEsto descargará ~2.8 millones de especies")
        print(f"Tamaño: ~500 MB - 1 GB comprimido")
        print(f"Espacio necesario: ~2-3 GB descomprimido")
        print(f"Tiempo estimado: 10-30 minutos (descarga) + 5-15 minutos (procesamiento)")
        print()
        
        # Descargar el archivo ZIP
        zip_path = self.output_dir / "backbone.zip"
        extract_dir = self.output_dir / "backbone_extract"
        
        if not zip_path.exists():
            print(f"Descargando desde: {self.backbone_url}")
            print("Este archivo es grande, puede tardar varios minutos...")
            print()
            
            try:
                # Descarga con barra de progreso
                response = requests.get(self.backbone_url, stream=True, timeout=60)
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                block_size = 8192
                downloaded = 0
                
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=block_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                mb_downloaded = downloaded / (1024 * 1024)
                                mb_total = total_size / (1024 * 1024)
                                print(f"\rDescargado: {mb_downloaded:.1f} MB / {mb_total:.1f} MB ({percent:.1f}%)", end='')
                
                print(f"\nDescarga completada: {zip_path}")
                
            except Exception as e:
                print(f"\nError descargando el archivo: {e}")
                if zip_path.exists():
                    zip_path.unlink()
                raise
        else:
            print(f"Archivo ya existe: {zip_path}")
        
        # Extraer el archivo
        print("\nExtrayendo archivos...")
        extract_dir.mkdir(exist_ok=True)
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            print(f"Archivos extraídos en: {extract_dir}")
        except Exception as e:
            print(f"Error extrayendo archivos: {e}")
            raise
        
        # Procesar el archivo Taxon.tsv (taxonomía completa)
        print("\nProcesando taxonomía completa...")
        taxon_file = extract_dir / "Taxon.tsv"
        
        if not taxon_file.exists():
            raise FileNotFoundError(f"No se encontró Taxon.tsv en {extract_dir}")
        
        # Leer el archivo TSV por chunks para no saturar la memoria
        print("Leyendo datos taxonómicos (esto puede tardar varios minutos)...")
        print("NOTA: El archivo puede tener líneas con formato inconsistente, se omitirán automáticamente")
        print()
        
        chunk_size = 50000  # Reducir tamaño de chunk para mejor manejo de errores
        all_species = []
        total_read = 0
        total_lines = 0
        errors_skipped = 0
        
        # Intentar con diferentes métodos de parsing
        parsing_methods = [
            {
                'name': 'Método 1: Python engine con skip',
                'params': {
                    'sep': '\t',
                    'chunksize': chunk_size,
                    'low_memory': False,
                    'encoding': 'utf-8',
                    'on_bad_lines': 'skip',
                    'engine': 'python',
                    'quoting': 3  # QUOTE_NONE
                }
            },
            {
                'name': 'Método 2: C engine con error_bad_lines',
                'params': {
                    'sep': '\t',
                    'chunksize': chunk_size,
                    'low_memory': False,
                    'encoding': 'utf-8',
                    'on_bad_lines': 'warn',
                    'engine': 'c'
                }
            }
        ]
        
        success = False
        
        for method in parsing_methods:
            print(f"Intentando {method['name']}...")
            all_species = []
            total_read = 0
            errors_skipped = 0
            
            try:
                for i, chunk in enumerate(pd.read_csv(taxon_file, **method['params'])):
                    try:
                        total_lines += len(chunk)
                        
                        # Verificar que tenemos las columnas necesarias
                        if 'taxonomicStatus' not in chunk.columns:
                            print(f"\nAdvertencia: No se encontró columna taxonomicStatus en chunk {i+1}")
                            continue
                        
                        # Filtrar solo especies aceptadas
                        if chunk['taxonomicStatus'].dtype == 'object':
                            species_chunk = chunk[
                                chunk['taxonomicStatus'].str.upper() == 'ACCEPTED'
                            ].copy()
                        else:
                            species_chunk = chunk[
                                chunk['taxonomicStatus'] == 'ACCEPTED'
                            ].copy()
                        
                        if len(species_chunk) == 0:
                            continue
                        
                        # Columnas que queremos extraer (con valores por defecto si no existen)
                        columns_mapping = {
                            'taxonID': 'species_key',
                            'scientificName': 'scientific_name',
                            'canonicalName': 'canonical_name',
                            'taxonRank': 'rank',
                            'kingdom': 'kingdom',
                            'phylum': 'phylum',
                            'class': 'class',
                            'order': 'order',
                            'family': 'family',
                            'genus': 'genus',
                            'specificEpithet': 'species',
                            'taxonomicStatus': 'taxonomic_status'
                        }
                        
                        # Extraer solo columnas disponibles
                        species_data = pd.DataFrame()
                        for old_col, new_col in columns_mapping.items():
                            if old_col in species_chunk.columns:
                                species_data[new_col] = species_chunk[old_col]
                            else:
                                species_data[new_col] = None
                        
                        all_species.append(species_data)
                        total_read += len(species_chunk)
                        
                        if (i + 1) % 10 == 0:
                            print(f"\rProcesadas {total_read:,} especies aceptadas de {total_lines:,} líneas totales...", end='')
                        
                    except Exception as e:
                        errors_skipped += 1
                        if errors_skipped <= 5:  # Mostrar solo los primeros 5 errores
                            print(f"\nAdvertencia en chunk {i+1}: {str(e)[:100]}")
                        continue
                
                print()
                
                # Si llegamos aquí y tenemos datos, fue exitoso
                if all_species:
                    success = True
                    print(f"{method['name']} exitoso")
                    break
                    
            except Exception as e:
                print(f"\n{method['name']} falló: {e}")
                continue
        
        if not success or not all_species:
            raise ValueError(
                "No se pudo procesar el archivo Taxon.tsv con ningún método. "
                "El archivo puede estar corrupto o tener un formato inesperado."
            )
        
        if errors_skipped > 0:
            print(f"\nNOTA: Se omitieron {errors_skipped} chunks con errores de formato")
            print(f"Esto es normal en el backbone de GBIF debido a caracteres especiales")
        
        # Combinar todos los chunks
        print("\nCombinando datos...")
        df = pd.concat(all_species, ignore_index=True)
        
        # Eliminar duplicados
        print("Eliminando duplicados...")
        df = df.drop_duplicates(subset=['species_key'], keep='first')
        
        # Añadir claves para cada nivel taxonómico
        print("Generando claves taxonómicas...")
        for rank in ['kingdom', 'phylum', 'class', 'order', 'family', 'genus']:
            if rank in df.columns:
                # Llenar valores nulos y convertir a string antes de factorize
                df[f'{rank}_key'] = df[rank].fillna('UNKNOWN').astype(str).factorize()[0]
        
        # Guardar
        output_file = self.output_dir / "gbif_taxonomy.csv"
        df.to_csv(output_file, index=False)
        
        print(f"\nTaxonomía guardada en {output_file}")
        print(f"Total de especies procesadas: {len(df):,}")
        
        # Limpiar archivos temporales si se desea
        print("\n¿Deseas eliminar los archivos extraídos para ahorrar espacio? (y/n)")
        print(f"Esto liberará ~2-3 GB pero conservará el ZIP y el CSV procesado")
        
        return df
    
    def download_gbif_backbone_taxonomy(self, limit: int = 100000):
        """
        Descarga la taxonomía backbone de GBIF
        
        Args:
            limit: Número máximo de especies a descargar (para pruebas, usa menos)
        """
        print(f"Descargando taxonomía backbone de GBIF (límite: {limit})...")
        
        # GBIF Backbone Taxonomy dataset key
        backbone_key = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"
        
        species_data = []
        offset = 0
        batch_size = 1000
        
        while offset < limit:
            url = f"{self.gbif_api}/species/search"
            params = {
                'datasetKey': backbone_key,
                'limit': min(batch_size, limit - offset),
                'offset': offset,
                'status': 'ACCEPTED'  # Solo especies aceptadas
            }
            
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('results'):
                    break
                
                for species in data['results']:
                    species_data.append({
                        'species_key': species.get('key'),
                        'scientific_name': species.get('scientificName'),
                        'canonical_name': species.get('canonicalName'),
                        'rank': species.get('rank'),
                        'kingdom': species.get('kingdom'),
                        'phylum': species.get('phylum'),
                        'class': species.get('class'),
                        'order': species.get('order'),
                        'family': species.get('family'),
                        'genus': species.get('genus'),
                        'species': species.get('species'),
                        'taxonomic_status': species.get('taxonomicStatus'),
                        'kingdom_key': species.get('kingdomKey'),
                        'phylum_key': species.get('phylumKey'),
                        'class_key': species.get('classKey'),
                        'order_key': species.get('orderKey'),
                        'family_key': species.get('familyKey'),
                        'genus_key': species.get('genusKey')
                    })
                
                offset += len(data['results'])
                print(f"Descargadas {offset} especies...")
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error en offset {offset}: {e}")
                time.sleep(5)
                continue
        
        df = pd.DataFrame(species_data)
        output_file = self.output_dir / "gbif_taxonomy.csv"
        df.to_csv(output_file, index=False)
        print(f"Taxonomía guardada en {output_file} ({len(df)} especies)")
        
        return df
    
    def download_gbif_backbone_taxonomy_unlimited(self):
        """
        Descarga toda la taxonomía disponible sin límite explícito.
        Continúa hasta que no haya más resultados disponibles del API.
        
        NOTA: El API de GBIF tiene límites internos (típicamente ~100K-200K registros).
        Para datasets completos con millones de especies, usa GBIF Download Service.
        """
        print("Descargando taxonomía backbone de GBIF (modo ilimitado)...")
        print("ADVERTENCIA: Esto puede tomar 30-60 minutos o más")
        print("El API tiene límites internos (~100K-200K especies máximo)\n")
        
        # GBIF Backbone Taxonomy dataset key
        backbone_key = "d7dddbf4-2cf0-4f39-9b2a-bb099caae36c"
        
        species_data = []
        offset = 0
        batch_size = 1000
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        while True:
            url = f"{self.gbif_api}/species/search"
            params = {
                'datasetKey': backbone_key,
                'limit': batch_size,
                'offset': offset,
                'status': 'ACCEPTED'  # Solo especies aceptadas
            }
            
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                results = data.get('results', [])
                
                # Si no hay resultados, hemos terminado
                if not results:
                    print(f"\nNo hay más resultados disponibles. Total descargado: {offset}")
                    break
                
                # Si obtenemos menos resultados de los esperados, estamos cerca del final
                if len(results) < batch_size:
                    print(f"\nÚltimo lote: {len(results)} especies")
                
                for species in results:
                    species_data.append({
                        'species_key': species.get('key'),
                        'scientific_name': species.get('scientificName'),
                        'canonical_name': species.get('canonicalName'),
                        'rank': species.get('rank'),
                        'kingdom': species.get('kingdom'),
                        'phylum': species.get('phylum'),
                        'class': species.get('class'),
                        'order': species.get('order'),
                        'family': species.get('family'),
                        'genus': species.get('genus'),
                        'species': species.get('species'),
                        'taxonomic_status': species.get('taxonomicStatus'),
                        'kingdom_key': species.get('kingdomKey'),
                        'phylum_key': species.get('phylumKey'),
                        'class_key': species.get('classKey'),
                        'order_key': species.get('orderKey'),
                        'family_key': species.get('familyKey'),
                        'genus_key': species.get('genusKey')
                    })
                
                offset += len(results)
                
                # Mostrar progreso cada 10K
                if offset % 10000 == 0:
                    print(f"Progreso: {offset:,} especies descargadas...")
                
                # Reset error counter on success
                consecutive_errors = 0
                
                # Rate limiting
                time.sleep(0.1)
                
            except Exception as e:
                consecutive_errors += 1
                print(f"Error en offset {offset}: {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    print(f"\nDemasiados errores consecutivos. Deteniendo en {offset} especies.")
                    break
                
                print(f"Reintentando en 5 segundos... (intento {consecutive_errors}/{max_consecutive_errors})")
                time.sleep(5)
                continue
        
        df = pd.DataFrame(species_data)
        output_file = self.output_dir / "gbif_taxonomy.csv"
        df.to_csv(output_file, index=False)
        print(f"\nTaxonomía guardada en {output_file} ({len(df):,} especies)")
        
        return df
    
    def download_occurrence_data(self, countries: List[str] = None, limit: int = 10000):
        """
        Descarga datos de ocurrencias geográficas de GBIF
        
        Args:
            countries: Lista de códigos ISO de países (ej: ['US', 'MX', 'BR'])
            limit: Número máximo de registros
        """
        print(f"Descargando datos de ocurrencias (límite: {limit})...")
        
        occurrences = []
        offset = 0
        batch_size = 300
        
        while offset < limit:
            url = f"{self.gbif_api}/occurrence/search"
            params = {
                'limit': min(batch_size, limit - offset),
                'offset': offset,
                'hasCoordinate': 'true',
                'hasGeospatialIssue': 'false'
            }
            
            if countries:
                params['country'] = ','.join(countries)
            
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                if not data.get('results'):
                    break
                
                for occ in data['results']:
                    occurrences.append({
                        'occurrence_key': occ.get('key'),
                        'species_key': occ.get('speciesKey'),
                        'scientific_name': occ.get('scientificName'),
                        'kingdom': occ.get('kingdom'),
                        'phylum': occ.get('phylum'),
                        'class': occ.get('class'),
                        'order': occ.get('order'),
                        'family': occ.get('family'),
                        'genus': occ.get('genus'),
                        'species': occ.get('species'),
                        'country': occ.get('country'),
                        'country_code': occ.get('countryCode'),
                        'state_province': occ.get('stateProvince'),
                        'locality': occ.get('locality'),
                        'latitude': occ.get('decimalLatitude'),
                        'longitude': occ.get('decimalLongitude'),
                        'continent': occ.get('continent'),
                        'year': occ.get('year')
                    })
                
                offset += len(data['results'])
                print(f"Descargadas {offset} ocurrencias...")
                
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error en offset {offset}: {e}")
                time.sleep(5)
                continue
        
        df = pd.DataFrame(occurrences)
        output_file = self.output_dir / "gbif_occurrences.csv"
        df.to_csv(output_file, index=False)
        print(f"Ocurrencias guardadas en {output_file} ({len(df)} registros)")
        
        return df
    
    def create_geographic_hierarchy(self):
        """
        Crea la jerarquía geográfica: Continente -> País -> Región
        Usa datos estándar de países y continentes
        """
        print("Creando jerarquía geográfica...")
        
        # Mapeo de países a continentes (ISO 3166-1 alpha-2)
        country_to_continent = {
            # América del Norte
            'US': 'North America', 'CA': 'North America', 'MX': 'North America',
            'GT': 'North America', 'BZ': 'North America', 'HN': 'North America',
            'SV': 'North America', 'NI': 'North America', 'CR': 'North America',
            'PA': 'North America', 'CU': 'North America', 'JM': 'North America',
            'HT': 'North America', 'DO': 'North America',
            
            # América del Sur
            'BR': 'South America', 'AR': 'South America', 'CL': 'South America',
            'CO': 'South America', 'PE': 'South America', 'VE': 'South America',
            'EC': 'South America', 'BO': 'South America', 'PY': 'South America',
            'UY': 'South America', 'GY': 'South America', 'SR': 'South America',
            'GF': 'South America',
            
            # Europa
            'GB': 'Europe', 'FR': 'Europe', 'DE': 'Europe', 'IT': 'Europe',
            'ES': 'Europe', 'PT': 'Europe', 'NL': 'Europe', 'BE': 'Europe',
            'PL': 'Europe', 'RO': 'Europe', 'GR': 'Europe', 'SE': 'Europe',
            'NO': 'Europe', 'FI': 'Europe', 'DK': 'Europe', 'CH': 'Europe',
            'AT': 'Europe', 'CZ': 'Europe', 'HU': 'Europe', 'IE': 'Europe',
            'RU': 'Europe',  # Parte europea de Rusia
            
            # Asia
            'CN': 'Asia', 'IN': 'Asia', 'JP': 'Asia', 'KR': 'Asia',
            'ID': 'Asia', 'TH': 'Asia', 'VN': 'Asia', 'PH': 'Asia',
            'MY': 'Asia', 'SG': 'Asia', 'PK': 'Asia', 'BD': 'Asia',
            'IR': 'Asia', 'IQ': 'Asia', 'SA': 'Asia', 'TR': 'Asia',
            'IL': 'Asia', 'AE': 'Asia', 'KZ': 'Asia', 'UZ': 'Asia',
            
            # África
            'ZA': 'Africa', 'EG': 'Africa', 'NG': 'Africa', 'KE': 'Africa',
            'ET': 'Africa', 'TZ': 'Africa', 'DZ': 'Africa', 'MA': 'Africa',
            'GH': 'Africa', 'CI': 'Africa', 'CM': 'Africa', 'UG': 'Africa',
            'SD': 'Africa', 'AO': 'Africa', 'MZ': 'Africa', 'MG': 'Africa',
            
            # Oceanía
            'AU': 'Oceania', 'NZ': 'Oceania', 'PG': 'Oceania', 'FJ': 'Oceania',
            'NC': 'Oceania', 'PF': 'Oceania', 'SB': 'Oceania', 'VU': 'Oceania',
            
            # Antártida
            'AQ': 'Antarctica'
        }
        
        # Obtener nombres completos de países
        country_names = self._get_country_names()
        
        geographic_data = []
        for country_code, continent in country_to_continent.items():
            country_name = country_names.get(country_code, country_code)
            geographic_data.append({
                'country_code': country_code,
                'country_name': country_name,
                'continent': continent
            })
        
        df = pd.DataFrame(geographic_data)
        output_file = self.output_dir / "geographic_hierarchy.csv"
        df.to_csv(output_file, index=False)
        print(f"Jerarquía geográfica guardada en {output_file} ({len(df)} países)")
        
        return df
    
    def _get_country_names(self) -> Dict[str, str]:
        """Obtiene nombres completos de países desde la API de REST Countries"""
        try:
            response = requests.get("https://restcountries.com/v3.1/all", timeout=10)
            response.raise_for_status()
            countries = response.json()
            
            return {
                country['cca2']: country['name']['common']
                for country in countries
                if 'cca2' in country
            }
        except Exception as e:
            print(f"No se pudieron obtener nombres de países: {e}")
            return {}
    
    def create_taxonomic_graph_structure(self, taxonomy_df: pd.DataFrame):
        """
        Crea estructura de nodos y relaciones para un grafo taxonómico
        """
        print("Creando estructura de grafo taxonómico...")
        
        nodes = []
        relationships = []
        node_ids = set()
        
        ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
        
        for _, row in taxonomy_df.iterrows():
            # Crear nodos para cada nivel taxonómico
            for rank in ranks:
                name = row.get(rank)
                key = row.get(f'{rank}_key') if rank != 'species' else row.get('species_key')
                
                if pd.notna(name) and name:
                    node_id = f"{rank}:{key}" if pd.notna(key) else f"{rank}:{name}"
                    
                    if node_id not in node_ids:
                        nodes.append({
                            'node_id': node_id,
                            'name': name,
                            'rank': rank,
                            'key': key
                        })
                        node_ids.add(node_id)
            
            # Crear relaciones jerárquicas
            for i in range(len(ranks) - 1):
                parent_rank = ranks[i]
                child_rank = ranks[i + 1]
                
                parent_name = row.get(parent_rank)
                child_name = row.get(child_rank)
                parent_key = row.get(f'{parent_rank}_key')
                child_key = row.get(f'{child_rank}_key') if child_rank != 'species' else row.get('species_key')
                
                if pd.notna(parent_name) and pd.notna(child_name):
                    parent_id = f"{parent_rank}:{parent_key}" if pd.notna(parent_key) else f"{parent_rank}:{parent_name}"
                    child_id = f"{child_rank}:{child_key}" if pd.notna(child_key) else f"{child_rank}:{child_name}"
                    
                    relationships.append({
                        'from_node': parent_id,
                        'to_node': child_id,
                        'relationship_type': 'HAS_CHILD',
                        'from_rank': parent_rank,
                        'to_rank': child_rank
                    })
        
        # Guardar nodos y relaciones
        nodes_df = pd.DataFrame(nodes)
        relationships_df = pd.DataFrame(relationships).drop_duplicates()
        
        nodes_file = self.output_dir / "taxonomy_nodes.csv"
        relationships_file = self.output_dir / "taxonomy_relationships.csv"
        
        nodes_df.to_csv(nodes_file, index=False)
        relationships_df.to_csv(relationships_file, index=False)
        
        print(f"Nodos taxonómicos: {nodes_file} ({len(nodes_df)} nodos)")
        print(f"Relaciones taxonómicas: {relationships_file} ({len(relationships_df)} relaciones)")
        
        return nodes_df, relationships_df
    
    def create_geographic_species_relationships(self, occurrences_df: pd.DataFrame):
        """
        Crea relaciones entre especies y ubicaciones geográficas
        """
        print("Creando relaciones especie-geografía...")
        
        relationships = []
        
        for _, row in occurrences_df.iterrows():
            if pd.notna(row['species_key']) and pd.notna(row['country_code']):
                relationships.append({
                    'species_key': row['species_key'],
                    'species_name': row['scientific_name'],
                    'country_code': row['country_code'],
                    'country': row['country'],
                    'continent': row['continent'],
                    'state_province': row['state_province'],
                    'latitude': row['latitude'],
                    'longitude': row['longitude']
                })
        
        df = pd.DataFrame(relationships).drop_duplicates(
            subset=['species_key', 'country_code']
        )
        
        output_file = self.output_dir / "species_geographic_relationships.csv"
        df.to_csv(output_file, index=False)
        print(f"Relaciones especie-geografía: {output_file} ({len(df)} relaciones)")
        
        return df
    
    def generate_summary_report(self):
        """Genera un reporte resumen de los datos descargados"""
        print("\n" + "="*60)
        print("RESUMEN DE DATOS DESCARGADOS")
        print("="*60)
        
        files = {
            'Taxonomía': 'gbif_taxonomy.csv',
            'Ocurrencias': 'gbif_occurrences.csv',
            'Jerarquía Geográfica': 'geographic_hierarchy.csv',
            'Nodos Taxonómicos': 'taxonomy_nodes.csv',
            'Relaciones Taxonómicas': 'taxonomy_relationships.csv',
            'Relaciones Especie-Geografía': 'species_geographic_relationships.csv'
        }
        
        for name, filename in files.items():
            filepath = self.output_dir / filename
            if filepath.exists():
                df = pd.read_csv(filepath)
                print(f"\n{name}:")
                print(f"  Archivo: {filepath}")
                print(f"  Registros: {len(df):,}")
                print(f"  Columnas: {', '.join(df.columns[:5])}...")
            else:
                print(f"\n{name}: No encontrado")
        
        print("\n" + "="*60)


def main():
    """Función principal para ejecutar la descarga completa"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Descargar datos de biodiversidad de GBIF"
    )
    parser.add_argument(
        "--full-backbone",
        action="store_true",
        help="Descargar el GBIF Backbone completo (~2.8 millones de especies, ~1 GB descarga)"
    )
    parser.add_argument(
        "--taxonomy-limit",
        type=int,
        default=None,
        help="Límite de especies a descargar via API (default: sin límite, descarga todo lo posible)"
    )
    parser.add_argument(
        "--occurrence-limit",
        type=int,
        default=100000,
        help="Límite de ocurrencias a descargar (default: 100000)"
    )
    parser.add_argument(
        "--countries",
        nargs='+',
        default=None,
        help="Códigos de países a filtrar (ej: --countries BR AR CL). Sin especificar = todos"
    )
    parser.add_argument(
        "--quick-test",
        action="store_true",
        help="Modo de prueba rápida (10K taxonomía, 5K ocurrencias)"
    )
    parser.add_argument(
        "--skip-occurrences",
        action="store_true",
        help="Saltar descarga de ocurrencias (solo taxonomía)"
    )
    
    args = parser.parse_args()
    
    # Configuración
    downloader = BiodiversityDataDownloader(output_dir="biodiversity_data")
    
    print("="*60)
    print("DESCARGA DE DATOS DE BIODIVERSIDAD")
    print("="*60)
    
    # Determinar modo de descarga
    if args.full_backbone:
        print("\nMODO: Backbone completo oficial")
        print("- Taxonomía: ~2.8 millones de especies")
        print("- Descarga: ~500 MB - 1 GB")
        print("- Espacio total: ~2-3 GB")
        print("- Tiempo: 15-45 minutos")
        use_full_backbone = True
        taxonomy_limit = None
        occurrence_limit = args.occurrence_limit  # Usar el valor del argumento
        print(f"- Ocurrencias: {occurrence_limit:,} registros")
    elif args.quick_test:
        print("\nMODO: Prueba rápida")
        print("- Taxonomía: 10,000 especies")
        print("- Ocurrencias: 5,000 registros")
        use_full_backbone = False
        taxonomy_limit = 10000
        occurrence_limit = 5000
    else:
        use_full_backbone = False
        taxonomy_limit = args.taxonomy_limit
        occurrence_limit = args.occurrence_limit
        
        if taxonomy_limit is None:
            print("\nMODO: Descarga máxima via API")
            print("- Taxonomía: Todo lo disponible via API (~100K-200K especies)")
            print("- NOTA: Para los 2.8M completos, usa --full-backbone")
        else:
            print(f"\nMODO: Límites personalizados")
            print(f"- Taxonomía: {taxonomy_limit:,} especies")
        
        print(f"- Ocurrencias: {occurrence_limit:,} registros")
    
    if args.skip_occurrences:
        print("- Ocurrencias: OMITIDAS")
    
    if args.countries:
        print(f"- Países filtrados: {', '.join(args.countries)}")
    else:
        print("- Países: Todos")
    
    print("\nEste script descargará:")
    if use_full_backbone:
        print("1. Backbone Taxonomy completo (archivo oficial ~1 GB)")
    else:
        print("1. Taxonomía de especies (GBIF API)")
    
    if not args.skip_occurrences:
        print("2. Datos de ocurrencias geográficas")
    print("3. Jerarquía geográfica (continentes, países, regiones)")
    print("4. Estructuras para grafo (nodos y relaciones)")
    print()
    
    # Confirmación para descarga completa
    if use_full_backbone:
        print("ADVERTENCIA: La descarga completa requiere:")
        print("- Tiempo: 15-45 minutos")
        print("- Espacio: ~2-3 GB")
        print("- Buena conexión a internet")
        print()
        response = input("¿Continuar con la descarga completa? (s/n): ")
        if response.lower() != 's':
            print("Descarga cancelada")
            return
        print()
    
    # 1. Descargar jerarquía geográfica
    geo_df = downloader.create_geographic_hierarchy()
    
    # 2. Descargar taxonomía
    if use_full_backbone:
        # Descargar backbone completo oficial
        taxonomy_df = downloader.download_complete_backbone()
    elif taxonomy_limit is None:
        # Descargar todo lo posible via API (sin límite explícito)
        taxonomy_df = downloader.download_gbif_backbone_taxonomy_unlimited()
    else:
        # Descargar con límite específico via API
        taxonomy_df = downloader.download_gbif_backbone_taxonomy(limit=taxonomy_limit)
    
    # 3. Descargar ocurrencias
    if not args.skip_occurrences:
        if args.quick_test:
            occurrence_limit = 5000
        
        occurrences_df = downloader.download_occurrence_data(
            countries=args.countries,
            limit=occurrence_limit
        )
    else:
        print("\nOmitiendo descarga de ocurrencias...")
        occurrences_df = pd.DataFrame()
    
    # 4. Crear estructura de grafo taxonómico
    print("\nCreando estructura de grafo taxonómico...")
    nodes_df, relationships_df = downloader.create_taxonomic_graph_structure(taxonomy_df)
    
    # 5. Crear relaciones especie-geografía
    if not args.skip_occurrences and not occurrences_df.empty:
        geo_species_df = downloader.create_geographic_species_relationships(occurrences_df)
    else:
        print("No hay ocurrencias para procesar relaciones geográficas")
    
    # 6. Generar reporte
    downloader.generate_summary_report()
    
    print("\n" + "="*60)
    print("DESCARGA COMPLETADA EXITOSAMENTE")
    print("="*60)
    print(f"\nTodos los archivos están en: {downloader.output_dir}/")
    print("\nPróximos pasos:")
    print("1. Revisar los archivos CSV generados")
    print("2. Importar los datos a Neo4j:")
    print("   python import_to_neo4j.py --clear")
    
    if use_full_backbone:
        print("\nNOTA: Con 2.8M especies, la importación a Neo4j puede tardar 1-2 horas")
        print("y requerir bastante RAM. Considera aumentar la memoria de Neo4j si es necesario.")


if __name__ == "__main__":
    main()