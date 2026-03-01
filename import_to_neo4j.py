"""
Script para importar datos de biodiversidad a Neo4j
Crea el grafo con taxonomía y geografía integrada

Solo usar en caso de que se quieran hacer imports parciales y no TODAS las especies de seres vivos

Si se desea hacer un import masivo, véase prepare_neo4j_import.py e instrucciones en el readme
"""

from neo4j import GraphDatabase
import pandas as pd
from pathlib import Path
from typing import List, Dict
import logging
from datetime import datetime
import time

# Configurar logging con timestamps y archivo
log_file = f"import_neo4j_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def load_secrets(secrets_file: str = ".secrets") -> Dict[str, str]:
    """
    Carga secretos desde un archivo .secrets usando python-dotenv
    
    Formato esperado del archivo:
    PASSWORD=tu_contraseña_aquí
    URI=bolt://localhost:7687
    USER=neo4j
    DATABASE=LinNeo
    
    Args:
        secrets_file: Ruta al archivo de secretos
    
    Returns:
        Diccionario con los secretos
    """
    from pathlib import Path
    import os
    
    secrets_path = Path(secrets_file)
    
    if not secrets_path.exists():
        logger.warning(f"Archivo {secrets_file} no encontrado")
        return {}
    
    try:
        # Usar python-dotenv para cargar las variables
        from dotenv import load_dotenv
        load_dotenv(secrets_file)
        
        # Leer las variables del ambiente
        secrets = {
            'PASSWORD': os.getenv('PASSWORD'),
            'URI': os.getenv('URI'),
            'USER': os.getenv('USER'),
            'DATABASE': os.getenv('DATABASE')
        }
        
        # Filtrar valores None
        secrets = {k: v for k, v in secrets.items() if v is not None}
        
        logger.info(f"Secretos cargados desde {secrets_file}")
        return secrets
        
    except ImportError:
        logger.error("python-dotenv no está instalado. Instala con: pip install python-dotenv")
        return {}
    except Exception as e:
        logger.warning(f"No se pudo leer {secrets_file}: {e}")
        return {}


class BiodiversityGraphImporter:
    """Importa datos de biodiversidad a Neo4j"""
    
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 user: str = "neo4j", 
                 password: str = "YOUR_PASSWORD_HERE",
                 database: str = "LinNeo",
                 data_dir: str = "biodiversity_data"):
        """
        Inicializa conexión a Neo4j
        
        Args:
            uri: URI de conexión a Neo4j
            user: Usuario de Neo4j
            password: Contraseña de Neo4j
            database: Nombre de la base de datos a usar (default: LinNeo)
            data_dir: Directorio con los archivos CSV
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.data_dir = Path(data_dir)
        
    def close(self):
        """Cierra la conexión a Neo4j"""
        self.driver.close()
    
    def clear_database(self):
        """Limpia la base de datos (CUIDADO! Elimina todo)"""
        start_time = time.time()
        with self.driver.session(database=self.database) as session:
            logger.info("Limpiando base de datos...")
            session.run("MATCH (n) DETACH DELETE n")
            elapsed = time.time() - start_time
            logger.info(f"Base de datos limpiada (tiempo: {elapsed:.2f}s)")
    
    def create_constraints(self):
        """Crea constraints e índices para mejor rendimiento"""
        start_time = time.time()
        with self.driver.session(database=self.database) as session:
            logger.info("Creando constraints e índices...")
            
            constraints = [
                # Taxonomía
                "CREATE CONSTRAINT species_key IF NOT EXISTS FOR (s:Species) REQUIRE s.key IS UNIQUE",
                "CREATE CONSTRAINT genus_name IF NOT EXISTS FOR (g:Genus) REQUIRE g.name IS UNIQUE",
                "CREATE CONSTRAINT family_name IF NOT EXISTS FOR (f:Family) REQUIRE f.name IS UNIQUE",
                "CREATE CONSTRAINT order_name IF NOT EXISTS FOR (o:Order) REQUIRE o.name IS UNIQUE",
                "CREATE CONSTRAINT class_name IF NOT EXISTS FOR (c:Class) REQUIRE c.name IS UNIQUE",
                "CREATE CONSTRAINT phylum_name IF NOT EXISTS FOR (p:Phylum) REQUIRE p.name IS UNIQUE",
                "CREATE CONSTRAINT kingdom_name IF NOT EXISTS FOR (k:Kingdom) REQUIRE k.name IS UNIQUE",
                
                # Geografía
                "CREATE CONSTRAINT continent_name IF NOT EXISTS FOR (c:Continent) REQUIRE c.name IS UNIQUE",
                "CREATE CONSTRAINT country_code IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE",
            ]
            
            for i, constraint in enumerate(constraints, 1):
                try:
                    session.run(constraint)
                    logger.debug(f"Constraint {i}/{len(constraints)} creado")
                except Exception as e:
                    logger.debug(f"Constraint {i} ya existe o error: {e}")
            
            elapsed = time.time() - start_time
            logger.info(f"Constraints creados (tiempo: {elapsed:.2f}s)")
    
    def import_geographic_hierarchy(self):
        """Importa la jerarquía geográfica: Continente -> País"""
        start_time = time.time()
        logger.info("Importando jerarquía geográfica...")
        
        geo_file = self.data_dir / "geographic_hierarchy.csv"
        df = pd.read_csv(geo_file)
        
        with self.driver.session(database=self.database) as session:
            # Crear continentes
            continents = df['continent'].unique()
            for continent in continents:
                if pd.notna(continent):
                    session.run(
                        "MERGE (c:Continent {name: $name})",
                        name=continent
                    )
            
            # Crear países y relaciones con continentes
            for _, row in df.iterrows():
                session.run("""
                    MERGE (country:Country {code: $code})
                    SET country.name = $name
                    WITH country
                    MATCH (continent:Continent {name: $continent})
                    MERGE (country)-[:PART_OF]->(continent)
                """, 
                    code=row['country_code'],
                    name=row['country_name'],
                    continent=row['continent']
                )
            
            elapsed = time.time() - start_time
            logger.info(f"Importados {len(continents)} continentes y {len(df)} países (tiempo: {elapsed:.2f}s)")
    
    def import_taxonomy_nodes(self):
        """Importa los nodos taxonómicos"""
        start_time = time.time()
        logger.info("Importando nodos taxonómicos...")
        
        nodes_file = self.data_dir / "taxonomy_nodes.csv"
        df = pd.read_csv(nodes_file)
        
        # Agrupar por rango taxonómico
        rank_labels = {
            'kingdom': 'Kingdom',
            'phylum': 'Phylum',
            'class': 'Class',
            'order': 'Order',
            'family': 'Family',
            'genus': 'Genus',
            'species': 'Species'
        }
        
        total_imported = 0
        
        with self.driver.session(database=self.database) as session:
            for rank, label in rank_labels.items():
                rank_start = time.time()
                rank_df = df[df['rank'] == rank]
                
                if len(rank_df) == 0:
                    continue
                
                logger.info(f"Importando {len(rank_df):,} nodos de {label}...")
                
                # Importar en batches pequeños con reintentos
                batch_size = 1000  # Reducido de 5000
                imported = 0
                
                for i in range(0, len(rank_df), batch_size):
                    batch = rank_df.iloc[i:i+batch_size]
                    
                    # Reintentar hasta 3 veces en caso de timeout
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            for _, row in batch.iterrows():
                                session.run(f"""
                                    MERGE (n:{label} {{name: $name}})
                                    SET n.key = $key,
                                        n.node_id = $node_id
                                """,
                                    name=row['name'],
                                    key=row['key'] if pd.notna(row['key']) else None,
                                    node_id=row['node_id']
                                )
                            
                            imported += len(batch)
                            
                            # Log cada 10K registros
                            if imported % 10000 == 0:
                                logger.info(f"  Progreso {label}: {imported:,}/{len(rank_df):,}")
                            
                            break  # Éxito, salir del retry loop
                            
                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(f"  Timeout en batch {i}, reintentando ({attempt+1}/{max_retries})...")
                                time.sleep(2)  # Esperar antes de reintentar
                            else:
                                logger.error(f"  Error después de {max_retries} intentos: {e}")
                                raise
                
                rank_elapsed = time.time() - rank_start
                logger.info(f"Importados {len(rank_df):,} nodos de {label} (tiempo: {rank_elapsed:.2f}s)")
                total_imported += len(rank_df)
        
        total_elapsed = time.time() - start_time
        logger.info(f"Total nodos importados: {total_imported:,} (tiempo total: {total_elapsed:.2f}s)")
    
    def import_taxonomy_relationships(self):
        """Importa las relaciones taxonómicas jerárquicas"""
        start_time = time.time()
        logger.info("Importando relaciones taxonómicas...")
        
        rels_file = self.data_dir / "taxonomy_relationships.csv"
        df = pd.read_csv(rels_file)
        
        logger.info(f"Total de relaciones a importar: {len(df):,}")
        
        rank_labels = {
            'kingdom': 'Kingdom',
            'phylum': 'Phylum',
            'class': 'Class',
            'order': 'Order',
            'family': 'Family',
            'genus': 'Genus',
            'species': 'Species'
        }
        
        with self.driver.session(database=self.database) as session:
            batch_size = 500  # Reducido para evitar timeouts
            processed = 0
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                # Reintentar hasta 3 veces
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        for _, row in batch.iterrows():
                            from_label = rank_labels[row['from_rank']]
                            to_label = rank_labels[row['to_rank']]
                            
                            session.run(f"""
                                MATCH (parent:{from_label} {{node_id: $from_node}})
                                MATCH (child:{to_label} {{node_id: $to_node}})
                                MERGE (parent)-[:HAS_CHILD]->(child)
                                MERGE (child)-[:BELONGS_TO]->(parent)
                            """,
                                from_node=row['from_node'],
                                to_node=row['to_node']
                            )
                        
                        processed += len(batch)
                        
                        # Log cada 10K relaciones
                        if processed % 10000 == 0:
                            elapsed = time.time() - start_time
                            rate = processed / elapsed if elapsed > 0 else 0
                            remaining = len(df) - processed
                            eta = remaining / rate if rate > 0 else 0
                            logger.info(f"  Procesadas {processed:,}/{len(df):,} relaciones ({rate:.0f} rel/s, ETA: {eta/60:.1f} min)")
                        
                        break  # Éxito
                        
                    except Exception as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"  Timeout en batch {i}, reintentando ({attempt+1}/{max_retries})...")
                            time.sleep(3)
                        else:
                            logger.error(f"  Error después de {max_retries} intentos en batch {i}: {e}")
                            # Continuar con el siguiente batch en lugar de fallar completamente
                            logger.warning(f"  Saltando batch {i}, continuando...")
                            break
        
        total_elapsed = time.time() - start_time
        logger.info(f"Relaciones importadas: {processed:,}/{len(df):,} (tiempo total: {total_elapsed:.2f}s)")
    
    def import_species_geographic_relationships(self):
        """Importa las relaciones entre especies y ubicaciones geográficas"""
        logger.info("Importando relaciones especie-geografía...")
        
        rels_file = self.data_dir / "species_geographic_relationships.csv"
        df = pd.read_csv(rels_file)
        
        with self.driver.session(database=self.database) as session:
            batch_size = 1000
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    if pd.notna(row['species_key']) and pd.notna(row['country_code']):
                        session.run("""
                            MATCH (species:Species {key: $species_key})
                            MATCH (country:Country {code: $country_code})
                            MERGE (species)-[r:FOUND_IN]->(country)
                            SET r.state_province = $state_province,
                                r.latitude = $latitude,
                                r.longitude = $longitude
                        """,
                            species_key=int(row['species_key']),
                            country_code=row['country_code'],
                            state_province=row['state_province'] if pd.notna(row['state_province']) else None,
                            latitude=float(row['latitude']) if pd.notna(row['latitude']) else None,
                            longitude=float(row['longitude']) if pd.notna(row['longitude']) else None
                        )
                
                logger.info(f"✓ Procesadas {min(i+batch_size, len(df))}/{len(df)} relaciones")
    
    def create_example_queries(self):
        """Ejecuta queries de ejemplo para verificar los datos"""
        logger.info("\n" + "="*60)
        logger.info("EJECUTANDO QUERIES DE EJEMPLO")
        logger.info("="*60)
        
        with self.driver.session(database=self.database) as session:
            # 1. Contar nodos por tipo
            logger.info("\n1. Conteo de nodos por tipo:")
            result = session.run("""
                MATCH (n)
                RETURN labels(n)[0] as tipo, count(*) as cantidad
                ORDER BY cantidad DESC
            """)
            for record in result:
                logger.info(f"   {record['tipo']}: {record['cantidad']:,}")
            
            # 2. Especies en Sudamérica
            logger.info("\n2. Especies encontradas en Sudamérica:")
            result = session.run("""
                MATCH (s:Species)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent {name: 'South America'})
                RETURN DISTINCT s.name as especie
                LIMIT 10
            """)
            for i, record in enumerate(result, 1):
                logger.info(f"   {i}. {record['especie']}")
            
            # 3. Países con más especies registradas
            logger.info("\n3. Top 5 países con más especies:")
            result = session.run("""
                MATCH (s:Species)-[:FOUND_IN]->(c:Country)
                RETURN c.name as pais, count(DISTINCT s) as num_especies
                ORDER BY num_especies DESC
                LIMIT 5
            """)
            for record in result:
                logger.info(f"   {record['pais']}: {record['num_especies']:,} especies")
            
            # 4. Jerarquía taxonómica de una especie
            logger.info("\n4. Ejemplo de jerarquía taxonómica:")
            result = session.run("""
                MATCH path = (s:Species)-[:BELONGS_TO*]->(k:Kingdom)
                WITH s, path
                LIMIT 1
                RETURN s.name as especie, 
                       [n in nodes(path) | labels(n)[0] + ': ' + n.name] as jerarquia
            """)
            for record in result:
                logger.info(f"   Especie: {record['especie']}")
                logger.info(f"   Jerarquía: {' -> '.join(reversed(record['jerarquia']))}")
        
        logger.info("\n" + "="*60)
    
    def import_all(self, clear_first: bool = False):
        """
        Importa todos los datos en orden correcto
        
        Args:
            clear_first: Si True, limpia la base de datos antes de importar
        """
        try:
            if clear_first:
                self.clear_database()
            
            self.create_constraints()
            self.import_geographic_hierarchy()
            self.import_taxonomy_nodes()
            self.import_taxonomy_relationships()
            self.import_species_geographic_relationships()
            
            logger.info("\n✓ Importación completada exitosamente!")
            
            # Ejecutar queries de ejemplo
            self.create_example_queries()
            
        except Exception as e:
            logger.error(f"Error durante la importación: {e}")
            raise
        finally:
            self.close()


def main():
    """Función principal"""
    import argparse
    
    # Cargar secretos desde archivo .secrets
    secrets = load_secrets(".secrets")
    
    parser = argparse.ArgumentParser(
        description="Importar datos de biodiversidad a Neo4j"
    )
    parser.add_argument(
        "--uri", 
        default=secrets.get("URI", "bolt://localhost:7687"),
        help="URI de Neo4j (default: desde .secrets o bolt://localhost:7687)"
    )
    parser.add_argument(
        "--user",
        default=secrets.get("USER", "neo4j"),
        help="Usuario de Neo4j (default: desde .secrets o neo4j)"
    )
    parser.add_argument(
        "--password",
        default=secrets.get("PASSWORD", None),
        help="Contraseña de Neo4j (default: desde .secrets o None)"
    )
    parser.add_argument(
        "--database",
        default=secrets.get("DATABASE", "LinNeo"),
        help="Nombre de la base de datos Neo4j (default: desde .secrets o LinNeo)"
    )
    parser.add_argument(
        "--data-dir",
        default="biodiversity_data",
        help="Directorio con los archivos CSV (default: biodiversity_data)"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Limpiar base de datos antes de importar"
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("IMPORTACION DE DATOS A NEO4J")
    print("="*60)
    print(f"\nConexion: {args.uri}")
    print(f"Usuario: {args.user}")
    print(f"Base de datos: {args.database}")
    print(f"Directorio de datos: {args.data_dir}")
    print(f"Limpiar antes: {'Si' if args.clear else 'No'}")
    print(f"Archivo de log: {log_file}")
    print()
    
    # Validar que existe la contraseña
    if not args.password:
        print("\nERROR: No se encontró contraseña.")
        print("\nOpciones:")
        print("1. Crear archivo .secrets con el formato:")
        print("   PASSWORD=tu_contraseña_aquí")
        print("   DATABASE=LinNeo")
        print("\n2. Usar el parámetro --password:")
        print("   python import_to_neo4j.py --password tu_contraseña")
        return
    
    print()
    
    importer = BiodiversityGraphImporter(
        uri=args.uri,
        user=args.user,
        password=args.password,
        database=args.database,
        data_dir=args.data_dir
    )
    
    importer.import_all(clear_first=args.clear)


if __name__ == "__main__":
    main()