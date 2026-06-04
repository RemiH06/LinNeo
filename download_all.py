#!/usr/bin/env python3
"""
LinNeo - Master Downloader
Ejecuta todos los fetchers de datos de biodiversidad en orden.

Estructura:
- data_fetchers/    <- Scripts individuales de cada fuente
- db_loaders/       <- Scripts Cypher para importar
- download_all.py   <- Este archivo (orquestador)

Uso:
    python download_all.py --all              # Ejecuta todos
    python download_all.py --wikidata         # Solo Wikidata
    python download_all.py --eol              # Solo EOL
    python download_all.py --list             # Lista disponibles
"""

import argparse
import sys
import io
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Callable

# ==================== CONFIGURACIÓN ====================

FETCHERS_DIR = Path("data_fetchers")
LOG_DIR = Path("biodiversity_data/.logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Logging (con encoding UTF-8 para Windows)
import sys
import io

# Fix para Windows: usar UTF-8 en consola
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "download_all.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==================== DISPONIBLE FETCHERS ====================

AVAILABLE_FETCHERS = {
    "wikidata": {
        "name": "Wikidata - Nombres Comunes",
        "description": "Descarga nombres comunes en múltiples idiomas",
        "module": "wikidata_fetcher",
        "function": "fetch_wikidata_common_names",
        "output": "biodiversity_data/wikidata/wikidata_common_names.csv",
        "time_estimate": "2-4 horas",
        "priority": "CRÍTICA"
    },
    "wikipedia": {
        "name": "Wikipedia - Descripciones",
        "description": "Descarga descripciones de texto completo",
        "module": "wikipedia_fetcher",
        "function": "fetch_wikipedia_descriptions",
        "output": "biodiversity_data/descriptions/wikipedia_descriptions.csv",
        "time_estimate": "varias horas",
        "priority": "CRÍTICA"
    },
    "eol": {
        "name": "EOL - Descripciones (respaldo)",
        "description": "Descripciones de especies sin articulo en Wikipedia",
        "module": "eol_fetcher",
        "function": "fetch_eol_descriptions",
        "output": "biodiversity_data/descriptions/eol_descriptions.csv",
        "time_estimate": "3-5 horas",
        "priority": "IMPORTANTE"
    },
    "fishbase": {
        "name": "FishBase - Peces",
        "description": "Descarga datos especializados de peces",
        "module": "fishbase_fetcher",
        "function": "fetch_fishbase_data",
        "output": "biodiversity_data/fishbase/fishbase_data.csv",
        "time_estimate": "2-3 horas",
        "priority": "IMPORTANTE"
    },
    "powo": {
        "name": "POWO - Plantas",
        "description": "Descarga datos de plantas (Kew Gardens)",
        "module": "powo_fetcher",
        "function": "fetch_powo_data",
        "output": "biodiversity_data/powo/powo_descriptions.csv",
        "time_estimate": "2-3 horas",
        "priority": "IMPORTANTE"
    },
    "amphibiaweb": {
        "name": "AmphibiaWeb - Anfibios",
        "description": "Descarga datos de anfibios",
        "module": "amphibiaweb_fetcher",
        "function": "fetch_amphibiaweb_data",
        "output": "biodiversity_data/amphibiaweb/amphibiaweb_data.csv",
        "time_estimate": "1-2 horas",
        "priority": "DESEABLE"
    },
    "xeno_canto": {
        "name": "Xeno-canto - Sonidos",
        "description": "Descarga URLs de sonidos de aves",
        "module": "xeno_canto_fetcher",
        "function": "fetch_xeno_canto_urls",
        "output": "biodiversity_data/xeno_canto/xeno_canto_sounds.csv",
        "time_estimate": "2-3 horas",
        "priority": "NICE-TO-HAVE"
    },
    "inaturalist": {
        "name": "iNaturalist - Imágenes",
        "description": "Descarga URLs de imágenes de especies",
        "module": "inaturalist_fetcher",
        "function": "fetch_inaturalist_images",
        "output": "biodiversity_data/inaturalist/inaturalist_images.csv",
        "time_estimate": "2-3 horas",
        "priority": "NICE-TO-HAVE"
    }
}

# Orden de ejecución (dependencias)
EXECUTION_ORDER = [
    "wikidata",      # Primero nombres comunes
    "wikipedia",     # Descripciones (principal)
    "eol",           # Descripciones (respaldo)
    "fishbase",      # Luego especializados
    "powo",
    "amphibiaweb",
    "xeno_canto",
    "inaturalist"
]


# ==================== FUNCIONES ====================

def import_fetcher(name: str) -> Callable:
    """
    Importa dinámicamente un fetcher.
    
    Args:
        name: Nombre del fetcher (ej: "wikidata")
    
    Returns:
        Función a ejecutar
    """
    
    if name not in AVAILABLE_FETCHERS:
        raise ValueError(f"Fetcher desconocido: {name}")
    
    fetcher_info = AVAILABLE_FETCHERS[name]
    module_name = fetcher_info["module"]
    function_name = fetcher_info["function"]
    
    # Intentar importar desde data_fetchers/
    try:
        module = __import__(f"data_fetchers.{module_name}", fromlist=[function_name])
        func = getattr(module, function_name)
        return func
    except ImportError:
        # Fallback: intentar desde raíz (para desarrollo)
        try:
            module = __import__(module_name, fromlist=[function_name])
            func = getattr(module, function_name)
            return func
        except ImportError as e:
            logger.error(f"No se pudo importar {module_name}.{function_name}: {e}")
            raise


def execute_fetcher(name: str) -> bool:
    """
    Ejecuta un fetcher específico.
    
    Args:
        name: Nombre del fetcher
    
    Returns:
        True si tuvo éxito, False si falló
    """
    
    if name not in AVAILABLE_FETCHERS:
        logger.error(f"Fetcher desconocido: {name}")
        return False
    
    fetcher_info = AVAILABLE_FETCHERS[name]
    
    logger.info("\n" + "=" * 70)
    logger.info(f"EJECUTANDO: {fetcher_info['name']}")
    logger.info("=" * 70)
    logger.info(f"Descripción: {fetcher_info['description']}")
    logger.info(f"Prioridad: {fetcher_info['priority']}")
    logger.info(f"Tiempo estimado: {fetcher_info['time_estimate']}")
    logger.info(f"Output: {fetcher_info['output']}")
    
    try:
        func = import_fetcher(name)
        func()
        
        logger.info(f"✓ {fetcher_info['name']} completado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"✗ Error en {name}: {e}", exc_info=True)
        return False


def list_fetchers():
    """Lista todos los fetchers disponibles."""
    
    print("\n" + "=" * 70)
    print("FETCHERS DISPONIBLES")
    print("=" * 70)
    
    for key in EXECUTION_ORDER:
        info = AVAILABLE_FETCHERS[key]
        print(f"\n{key.upper()}")
        print(f"  Nombre: {info['name']}")
        print(f"  Descripción: {info['description']}")
        print(f"  Prioridad: {info['priority']}")
        print(f"  Tiempo: {info['time_estimate']}")
        print(f"  Output: {info['output']}")
    
    print("\n" + "=" * 70)


def run_all_fetchers() -> Dict[str, bool]:
    """Ejecuta todos los fetchers en orden."""
    
    results = {}
    
    logger.info("\n" + "=" * 70)
    logger.info("INICIANDO DESCARGA COMPLETA DE DATOS")
    logger.info("=" * 70)
    logger.info(f"Inicio: {datetime.now()}")
    logger.info(f"Fetchers a ejecutar: {len(EXECUTION_ORDER)}")
    
    for i, fetcher_name in enumerate(EXECUTION_ORDER, 1):
        logger.info(f"\n[{i}/{len(EXECUTION_ORDER)}] Ejecutando {fetcher_name}...")
        results[fetcher_name] = execute_fetcher(fetcher_name)
    
    # Resumen final
    print("\n" + "=" * 70)
    print("RESUMEN DE EJECUCIÓN")
    print("=" * 70)
    
    successful = sum(1 for v in results.values() if v)
    failed = sum(1 for v in results.values() if not v)
    
    for fetcher_name, success in results.items():
        status = "✓" if success else "✗"
        print(f"{status} {AVAILABLE_FETCHERS[fetcher_name]['name']}")
    
    print(f"\nTotal: {successful} exitosos, {failed} fallidos")
    print(f"Fin: {datetime.now()}")
    print("=" * 70)
    
    return results


# ==================== MAIN ====================

def main():
    parser = argparse.ArgumentParser(
        description="LinNeo - Master Data Fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python download_all.py --all              # Ejecuta todos
  python download_all.py --wikidata         # Solo Wikidata
  python download_all.py --list             # Lista disponibles
        """
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Ejecuta todos los fetchers"
    )
    
    parser.add_argument(
        "--list",
        action="store_true",
        help="Lista fetchers disponibles"
    )
    
    parser.add_argument(
        "--wikidata",
        action="store_true",
        help="Ejecuta solo Wikidata"
    )
    
    parser.add_argument(
        "--wikipedia",
        action="store_true",
        help="Ejecuta solo Wikipedia"
    )
    
    parser.add_argument(
        "--eol",
        action="store_true",
        help="Ejecuta solo EOL"
    )
    
    parser.add_argument(
        "--fishbase",
        action="store_true",
        help="Ejecuta solo FishBase"
    )
    
    parser.add_argument(
        "--powo",
        action="store_true",
        help="Ejecuta solo POWO"
    )
    
    parser.add_argument(
        "--amphibiaweb",
        action="store_true",
        help="Ejecuta solo AmphibiaWeb"
    )
    
    parser.add_argument(
        "--xeno-canto",
        action="store_true",
        help="Ejecuta solo Xeno-canto"
    )
    
    parser.add_argument(
        "--inaturalist",
        action="store_true",
        help="Ejecuta solo iNaturalist"
    )
    
    args = parser.parse_args()
    
    # Si no hay argumentos
    if not any([args.all, args.list, args.wikidata, args.wikipedia, args.eol, args.fishbase, 
                args.powo, args.amphibiaweb, args.xeno_canto, args.inaturalist]):
        parser.print_help()
        return
    
    # Listar
    if args.list:
        list_fetchers()
        return
    
    # Ejecutar
    if args.all:
        run_all_fetchers()
    else:
        # Ejecución selectiva
        results = {}
        
        if args.wikidata:
            results["wikidata"] = execute_fetcher("wikidata")
        if args.wikipedia:
            results["wikipedia"] = execute_fetcher("wikipedia")
        if args.eol:
            results["eol"] = execute_fetcher("eol")
        if args.fishbase:
            results["fishbase"] = execute_fetcher("fishbase")
        if args.powo:
            results["powo"] = execute_fetcher("powo")
        if args.amphibiaweb:
            results["amphibiaweb"] = execute_fetcher("amphibiaweb")
        if args.xeno_canto:
            results["xeno_canto"] = execute_fetcher("xeno_canto")
        if args.inaturalist:
            results["inaturalist"] = execute_fetcher("inaturalist")
        
        # Resumen
        print(f"\nResultados: {sum(results.values())}/{len(results)} exitosos")


if __name__ == "__main__":
    main()