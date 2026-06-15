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
    
    # Mapeo completo ISO-A2 -> continente (242 paises/territorios).
    # Reemplaza al antiguo diccionario de ~93 paises hecho a mano.
    COUNTRY_TO_CONTINENT = {
        'AD':'Europe','AE':'Asia','AF':'Asia','AG':'North America','AI':'North America','AL':'Europe','AM':'Asia','AO':'Africa','AQ':'Antarctica','AR':'South America','AS':'Oceania','AT':'Europe','AU':'Oceania','AW':'North America','AX':'Europe','AZ':'Asia',
        'BA':'Europe','BB':'North America','BD':'Asia','BE':'Europe','BF':'Africa','BG':'Europe','BH':'Asia','BI':'Africa','BJ':'Africa','BL':'North America','BM':'North America','BN':'Asia','BO':'South America','BQ':'North America','BR':'South America','BS':'North America','BT':'Asia','BV':'Antarctica','BW':'Africa','BY':'Europe','BZ':'North America',
        'CA':'North America','CC':'Asia','CD':'Africa','CF':'Africa','CG':'Africa','CH':'Europe','CI':'Africa','CK':'Oceania','CL':'South America','CM':'Africa','CN':'Asia','CO':'South America','CR':'North America','CU':'North America','CV':'Africa','CW':'North America','CX':'Asia','CY':'Asia','CZ':'Europe',
        'DE':'Europe','DJ':'Africa','DK':'Europe','DM':'North America','DO':'North America','DZ':'Africa',
        'EC':'South America','EE':'Europe','EG':'Africa','EH':'Africa','ER':'Africa','ES':'Europe','ET':'Africa',
        'FI':'Europe','FJ':'Oceania','FK':'South America','FM':'Oceania','FO':'Europe','FR':'Europe',
        'GA':'Africa','GB':'Europe','GD':'North America','GE':'Asia','GF':'South America','GG':'Europe','GH':'Africa','GI':'Europe','GL':'North America','GM':'Africa','GN':'Africa','GP':'North America','GQ':'Africa','GR':'Europe','GS':'Antarctica','GT':'North America','GU':'Oceania','GW':'Africa','GY':'South America',
        'HK':'Asia','HM':'Antarctica','HN':'North America','HR':'Europe','HT':'North America','HU':'Europe',
        'ID':'Asia','IE':'Europe','IL':'Asia','IM':'Europe','IN':'Asia','IO':'Asia','IQ':'Asia','IR':'Asia','IS':'Europe','IT':'Europe',
        'JE':'Europe','JM':'North America','JO':'Asia','JP':'Asia',
        'KE':'Africa','KG':'Asia','KH':'Asia','KI':'Oceania','KM':'Africa','KN':'North America','KP':'Asia','KR':'Asia','KW':'Asia','KY':'North America','KZ':'Asia',
        'LA':'Asia','LB':'Asia','LC':'North America','LI':'Europe','LK':'Asia','LR':'Africa','LS':'Africa','LT':'Europe','LU':'Europe','LV':'Europe','LY':'Africa',
        'MA':'Africa','MC':'Europe','MD':'Europe','ME':'Europe','MF':'North America','MG':'Africa','MH':'Oceania','MK':'Europe','ML':'Africa','MM':'Asia','MN':'Asia','MO':'Asia','MP':'Oceania','MQ':'North America','MR':'Africa','MS':'North America','MT':'Europe','MU':'Africa','MV':'Asia','MW':'Africa','MX':'North America','MY':'Asia','MZ':'Africa',
        'NA':'Africa','NC':'Oceania','NE':'Africa','NF':'Oceania','NG':'Africa','NI':'North America','NL':'Europe','NO':'Europe','NP':'Asia','NR':'Oceania','NU':'Oceania','NZ':'Oceania',
        'OM':'Asia',
        'PA':'North America','PE':'South America','PF':'Oceania','PG':'Oceania','PH':'Asia','PK':'Asia','PL':'Europe','PM':'North America','PN':'Oceania','PR':'North America','PS':'Asia','PT':'Europe','PW':'Oceania','PY':'South America',
        'QA':'Asia',
        'RE':'Africa','RO':'Europe','RS':'Europe','RU':'Europe','RW':'Africa',
        'SA':'Asia','SB':'Oceania','SC':'Africa','SD':'Africa','SE':'Europe','SG':'Asia','SH':'Africa','SI':'Europe','SJ':'Europe','SK':'Europe','SL':'Africa','SM':'Europe','SN':'Africa','SO':'Africa','SR':'South America','SS':'Africa','ST':'Africa','SV':'North America','SX':'North America','SY':'Asia','SZ':'Africa',
        'TC':'North America','TD':'Africa','TF':'Antarctica','TG':'Africa','TH':'Asia','TJ':'Asia','TK':'Oceania','TL':'Asia','TM':'Asia','TN':'Africa','TO':'Oceania','TR':'Asia','TT':'North America','TV':'Oceania','TW':'Asia','TZ':'Africa',
        'UA':'Europe','UG':'Africa','UM':'Oceania','US':'North America','UY':'South America','UZ':'Asia',
        'VA':'Europe','VC':'North America','VE':'South America','VG':'North America','VI':'North America','VN':'Asia','VU':'Oceania',
        'WF':'Oceania','WS':'Oceania',
        'XK':'Europe',
        'YE':'Asia','YT':'Africa',
        'ZA':'Africa','ZM':'Africa','ZW':'Africa',
    }

    # threatStatus (texto IUCN) -> (codigo, severidad). Mayor severidad = peor.
    IUCN_STATUS = {
        "least concern":         ("LC", 1),
        "near threatened":       ("NT", 2),
        "vulnerable":            ("VU", 3),
        "endangered":            ("EN", 4),
        "critically endangered": ("CR", 5),
        "regionally extinct":    ("RE", 6),
        "extinct in the wild":   ("EW", 7),
        "extinct":               ("EX", 8),
        "data deficient":        ("DD", 0),
        "not evaluated":         ("NE", 0),
        "not applicable":        ("NA", 0),
    }

    # Prioridad de establishmentMeans al colapsar (especie, pais) duplicados.
    EM_PRIORITY = {
        "native": 6, "naturalised": 5, "invasive": 4,
        "introduced": 3, "uncertain": 2, "managed": 1, "": 0,
    }

    def create_geographic_hierarchy(self, country_codes: List[str] = None):
        """
        Genera DIRECTAMENTE los CSV finales de geografia que consume el loader:
          - continent_nodes.csv        (name)
          - country_nodes.csv          (key, name)   key=name=codigo ISO
          - country_continent_rels.csv (country_key, continent_name)

        Usa el mapeo completo de 242 paises (COUNTRY_TO_CONTINENT). Si se pasan
        country_codes (los presentes en la distribucion), solo incluye esos.
        """
        print("Creando jerarquía geográfica (archivos finales)...")

        codes = sorted(set(country_codes)) if country_codes else sorted(self.COUNTRY_TO_CONTINENT.keys())

        sin_continente = [c for c in codes if c not in self.COUNTRY_TO_CONTINENT]
        if sin_continente:
            print(f"  AVISO: paises sin continente mapeado: {sin_continente}")

        # continent_nodes.csv
        continents = sorted(set(self.COUNTRY_TO_CONTINENT.get(c, 'Unknown') for c in codes))
        pd.DataFrame({'name': continents}).to_csv(
            self.output_dir / "continent_nodes.csv", index=False)

        # country_nodes.csv (key = name = codigo ISO, compatible con el mapa del frontend)
        pd.DataFrame({'key': codes, 'name': codes}).to_csv(
            self.output_dir / "country_nodes.csv", index=False)

        # country_continent_rels.csv
        rels = [{'country_key': c, 'continent_name': self.COUNTRY_TO_CONTINENT.get(c, 'Unknown')} for c in codes]
        pd.DataFrame(rels).to_csv(
            self.output_dir / "country_continent_rels.csv", index=False)

        print(f"  -> continent_nodes.csv ({len(continents)}), country_nodes.csv ({len(codes)}), country_continent_rels.csv")
        return pd.DataFrame(rels)
    
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
    
    def create_geographic_species_relationships(self, occurrences_df=None):
        """
        Crea las relaciones especie-pais a partir del Distribution.tsv del backbone
        de GBIF (no de la API de ocurrencias, que solo daba avistamientos y ~93 paises).

        El Distribution.tsv trae los 242 paises y datos curados:
          - establishmentMeans: native / introduced / naturalised / invasive / managed / uncertain
          - occurrenceStatus:   present / absent / doubtful / excluded / irregular / rare / common
          - threatStatus (IUCN): estado de conservacion, POR PAIS

        Genera:
          - species_geographic_relationships.csv  (relacion enriquecida)
          - species_conservation.csv              (peor estado por especie)
        Y devuelve la lista de codigos de pais encontrados (para la jerarquia).

        El parametro occurrences_df se ignora (se mantiene por compatibilidad de firma).
        """
        print("Creando relaciones especie-geografía desde Distribution.tsv...")

        tsv_path = self.output_dir / "backbone_extract" / "Distribution.tsv"
        if not tsv_path.exists():
            # buscar cualquier *Distribution.tsv dentro de backbone_extract
            extract_dir = self.output_dir / "backbone_extract"
            candidates = list(extract_dir.glob("*Distribution.tsv")) if extract_dir.exists() else []
            if candidates:
                tsv_path = candidates[0]
            else:
                print(f"  ERROR: no se encontró Distribution.tsv en {extract_dir}")
                return pd.DataFrame()

        print(f"  Leyendo {tsv_path} (puede tardar, ~471MB)...")
        usecols = ['taxonID', 'countryCode', 'establishmentMeans', 'occurrenceStatus', 'threatStatus']
        df = pd.read_csv(tsv_path, sep='\t', dtype=str, keep_default_na=False, usecols=usecols)
        print(f"  {len(df):,} filas leídas")

        df = df[(df['taxonID'] != '') & (df['countryCode'] != '')].copy()
        df['countryCode'] = df['countryCode'].str.upper().str.strip()
        df['establishmentMeans'] = df['establishmentMeans'].str.lower().str.strip()
        df['occurrenceStatus'] = df['occurrenceStatus'].str.lower().str.strip()
        df['threatStatus'] = df['threatStatus'].str.lower().str.strip()

        df['species_key'] = pd.to_numeric(df['taxonID'], errors='coerce')
        df = df.dropna(subset=['species_key'])
        df['species_key'] = df['species_key'].astype(int)

        df['cons_code'] = df['threatStatus'].map(lambda t: self.IUCN_STATUS.get(t, ('', 0))[0])
        df['cons_sev'] = df['threatStatus'].map(lambda t: self.IUCN_STATUS.get(t, ('', 0))[1])
        df['em_prio'] = df['establishmentMeans'].map(lambda e: self.EM_PRIORITY.get(e, 0))

        # Dedup por (especie, pais): mayor prioridad de establishmentMeans y, en empate, peor conservacion
        df_sorted = df.sort_values(['em_prio', 'cons_sev'], ascending=False)
        dist = df_sorted.drop_duplicates(subset=['species_key', 'countryCode'], keep='first')
        print(f"  {len(dist):,} pares (especie, país) únicos en {dist['countryCode'].nunique()} países")

        out = dist[['species_key', 'countryCode', 'establishmentMeans',
                    'occurrenceStatus', 'threatStatus', 'cons_code']].rename(columns={
            'countryCode': 'country_key',
            'establishmentMeans': 'establishment_means',
            'occurrenceStatus': 'occurrence_status',
            'threatStatus': 'conservation_status',
            'cons_code': 'conservation_code',
        })
        output_file = self.output_dir / "species_country_rels.csv"
        out.to_csv(output_file, index=False)
        print(f"Relaciones especie-país: {output_file} ({len(out):,} relaciones)")

        # Estado de conservacion general por especie (peor caso)
        idx = df.groupby('species_key')['cons_sev'].idxmax()
        cons = df.loc[idx]
        cons = cons[cons['cons_sev'] > 0]
        cons_out = cons[['species_key', 'threatStatus', 'cons_code']].rename(columns={
            'threatStatus': 'conservation_overall',
            'cons_code': 'conservation_overall_code',
        })
        cons_file = self.output_dir / "species_conservation.csv"
        cons_out.to_csv(cons_file, index=False)
        print(f"Conservación por especie: {cons_file} ({len(cons_out):,} especies)")

        # devolver los codigos de pais para construir la jerarquia acorde
        self._distribution_country_codes = sorted(dist['countryCode'].unique())
        return out
    
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
    
    # 3. (Opcional) ocurrencias por API -- ya NO se usan para la distribucion,
    #    que ahora sale del Distribution.tsv del backbone. Se mantiene por si
    #    quieres puntos de ocurrencia para otra cosa.
    if not args.skip_occurrences:
        if args.quick_test:
            occurrence_limit = 5000
        occurrences_df = downloader.download_occurrence_data(
            countries=args.countries,
            limit=occurrence_limit
        )
    else:
        print("\nOmitiendo descarga de ocurrencias (la distribución sale del backbone)...")
        occurrences_df = pd.DataFrame()
    
    # 4. Crear estructura de grafo taxonómico
    print("\nCreando estructura de grafo taxonómico...")
    nodes_df, relationships_df = downloader.create_taxonomic_graph_structure(taxonomy_df)
    
    # 5. Relaciones especie-geografía desde Distribution.tsv (242 países, datos curados)
    geo_species_df = downloader.create_geographic_species_relationships()

    # 6. Jerarquía geográfica acorde a los países encontrados en la distribución
    country_codes = getattr(downloader, '_distribution_country_codes', None)
    downloader.create_geographic_hierarchy(country_codes=country_codes)
    
    # 7. Generar reporte
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