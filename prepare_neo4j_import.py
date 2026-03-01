"""
Script para preparar archivos CSV para neo4j-admin import
Convierte los CSVs de GBIF al formato requerido por neo4j-admin

Recomiendo fuertemente modificar con ruta absoluta el comando generado en caso de ser necesario
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


def prepare_nodes_for_import(data_dir: str = "biodiversity_data", output_dir: str = "neo4j_import"):
    """
    Convierte los nodos taxonómicos al formato de neo4j-admin import
    
    Formato requerido:
    - Header con :ID, :LABEL, propiedades
    - Un archivo por tipo de nodo
    """
    data_path = Path(data_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    logger.info("Preparando archivos para neo4j-admin import...")
    
    # 1. Leer nodos taxonómicos
    logger.info("Procesando nodos taxonómicos...")
    nodes_file = data_path / "taxonomy_nodes.csv"
    df = pd.read_csv(nodes_file)
    
    rank_labels = {
        'kingdom': 'Kingdom',
        'phylum': 'Phylum',
        'class': 'Class',
        'order': 'Order',
        'family': 'Family',
        'genus': 'Genus',
        'species': 'Species'
    }
    
    # Crear un archivo por cada tipo de nodo
    for rank, label in rank_labels.items():
        rank_df = df[df['rank'] == rank].copy()
        
        if len(rank_df) == 0:
            continue
        
        # Renombrar columnas al formato neo4j-admin
        rank_df = rank_df.rename(columns={
            'node_id': 'nodeId:ID',
            'name': 'name',
            'key': 'key:long',
            'rank': 'rank'
        })
        
        # Eliminar columna rank ya que el label indica el tipo
        rank_df = rank_df.drop(columns=['rank'])
        
        # Guardar con header especial
        output_file = output_path / f"nodes_{rank}.csv"
        rank_df.to_csv(output_file, index=False)
        logger.info(f"  {label}: {len(rank_df):,} nodos -> {output_file}")
    
    # 2. Procesar geografía
    logger.info("Procesando nodos geográficos...")
    
    # Continentes
    geo_file = data_path / "geographic_hierarchy.csv"
    geo_df = pd.read_csv(geo_file)
    
    continents = geo_df[['continent']].drop_duplicates().copy()
    continents.columns = ['name']
    continents['continentId:ID'] = continents['name'].str.replace(' ', '_')
    continents = continents[['continentId:ID', 'name']]
    continents.to_csv(output_path / "nodes_continent.csv", index=False)
    logger.info(f"  Continent: {len(continents)} nodos -> nodes_continent.csv")
    
    # Países
    countries = geo_df[['country_code', 'country_name']].copy()
    countries.columns = ['countryId:ID', 'name']
    countries.to_csv(output_path / "nodes_country.csv", index=False)
    logger.info(f"  Country: {len(countries)} nodos -> nodes_country.csv")
    
    logger.info("Nodos preparados")


def prepare_relationships_for_import(data_dir: str = "biodiversity_data", output_dir: str = "neo4j_import"):
    """
    Convierte las relaciones al formato de neo4j-admin import
    
    Formato requerido:
    - :START_ID, :END_ID, :TYPE, propiedades
    """
    data_path = Path(data_dir)
    output_path = Path(output_dir)
    
    logger.info("Procesando relaciones taxonómicas...")
    
    # Relaciones taxonómicas
    rels_file = data_path / "taxonomy_relationships.csv"
    df = pd.read_csv(rels_file)
    
    # HAS_CHILD
    has_child = df[['from_node', 'to_node']].copy()
    has_child.columns = [':START_ID', ':END_ID']
    has_child[':TYPE'] = 'HAS_CHILD'
    has_child.to_csv(output_path / "rels_has_child.csv", index=False)
    logger.info(f"  HAS_CHILD: {len(has_child):,} relaciones")
    
    # BELONGS_TO
    belongs_to = df[['to_node', 'from_node']].copy()  # Invertido
    belongs_to.columns = [':START_ID', ':END_ID']
    belongs_to[':TYPE'] = 'BELONGS_TO'
    belongs_to.to_csv(output_path / "rels_belongs_to.csv", index=False)
    logger.info(f"  BELONGS_TO: {len(belongs_to):,} relaciones")
    
    # Relaciones geográficas
    logger.info("Procesando relaciones geográficas...")
    
    geo_file = data_path / "geographic_hierarchy.csv"
    geo_df = pd.read_csv(geo_file)
    
    # PART_OF (Country -> Continent)
    part_of = geo_df[['country_code', 'continent']].copy()
    part_of['continent'] = part_of['continent'].str.replace(' ', '_')
    part_of.columns = [':START_ID', ':END_ID']
    part_of[':TYPE'] = 'PART_OF'
    part_of.to_csv(output_path / "rels_part_of.csv", index=False)
    logger.info(f"  PART_OF: {len(part_of):,} relaciones")
    
    logger.info("Relaciones preparadas")


def generate_import_command(output_dir: str = "neo4j_import"):
    """
    Genera el comando neo4j-admin import
    """
    output_path = Path(output_dir)
    
    logger.info("\n" + "="*60)
    logger.info("ARCHIVOS PREPARADOS PARA IMPORTACIÓN")
    logger.info("="*60)
    
    # Listar archivos generados
    node_files = sorted(output_path.glob("nodes_*.csv"))
    rel_files = sorted(output_path.glob("rels_*.csv"))
    
    logger.info(f"\nArchivos de nodos ({len(node_files)}):")
    for f in node_files:
        size = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name} ({size:.1f} MB)")
    
    logger.info(f"\nArchivos de relaciones ({len(rel_files)}):")
    for f in rel_files:
        size = f.stat().st_size / (1024 * 1024)
        logger.info(f"  {f.name} ({size:.1f} MB)")
    
    logger.info("\n" + "="*60)
    logger.info("SIGUIENTE PASO: EJECUTAR IMPORTACIÓN")
    logger.info("="*60)
    
    # Construir comando
    cmd_parts = ["neo4j-admin database import full LinNeo"]
    
    # Agregar nodos
    cmd_parts.append("  --nodes=Kingdom=neo4j_import/nodes_kingdom.csv")
    cmd_parts.append("  --nodes=Phylum=neo4j_import/nodes_phylum.csv")
    cmd_parts.append("  --nodes=Class=neo4j_import/nodes_class.csv")
    cmd_parts.append("  --nodes=Order=neo4j_import/nodes_order.csv")
    cmd_parts.append("  --nodes=Family=neo4j_import/nodes_family.csv")
    cmd_parts.append("  --nodes=Genus=neo4j_import/nodes_genus.csv")
    cmd_parts.append("  --nodes=Species=neo4j_import/nodes_species.csv")
    cmd_parts.append("  --nodes=Continent=neo4j_import/nodes_continent.csv")
    cmd_parts.append("  --nodes=Country=neo4j_import/nodes_country.csv")
    
    # Agregar relaciones
    cmd_parts.append("  --relationships=neo4j_import/rels_has_child.csv")
    cmd_parts.append("  --relationships=neo4j_import/rels_belongs_to.csv")
    cmd_parts.append("  --relationships=neo4j_import/rels_part_of.csv")
    
    # Opciones adicionales
    cmd_parts.append("  --overwrite-destination")
    
    command = " \\\n".join(cmd_parts)
    
    logger.info("\nComando para Windows PowerShell:")
    logger.info("-" * 60)
    # Para PowerShell, usar backtick en lugar de backslash
    ps_command = command.replace(" \\", " `")
    logger.info(ps_command)
    
    logger.info("\nComando para Git Bash / Linux / Mac:")
    logger.info("-" * 60)
    logger.info(command)
    
    # Guardar en archivo
    with open(output_path / "import_command.txt", "w") as f:
        f.write("# Para Windows PowerShell:\n")
        f.write(ps_command + "\n\n")
        f.write("# Para Git Bash / Linux / Mac:\n")
        f.write(command + "\n")
    
    logger.info(f"\nComando guardado en: {output_path / 'import_command.txt'}")
    
    logger.info("\n" + "="*60)
    logger.info("INSTRUCCIONES:")
    logger.info("="*60)
    logger.info("1. La instancia Neo4j DEBE estar DETENIDA")
    logger.info("2. Abre una terminal en el directorio de Neo4j")
    logger.info("3. Ejecuta el comando de arriba")
    logger.info("4. Espera ~5-15 minutos (mucho más rápido)")
    logger.info("5. Inicia la instancia Neo4j")
    logger.info("="*60)


def main():
    logger.info("="*60)
    logger.info("PREPARACIÓN DE DATOS PARA NEO4J-ADMIN IMPORT")
    logger.info("="*60)
    
    # Preparar archivos
    prepare_nodes_for_import()
    prepare_relationships_for_import()
    generate_import_command()
    
    logger.info("\n¡Listo! Ahora ejecuta el comando generado")


if __name__ == "__main__":
    main()