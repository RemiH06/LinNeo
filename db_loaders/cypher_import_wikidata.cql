// ============================================================
// LinNeo -- Import Wikidata Common Names
// ============================================================
//
// REQUISITO: Ejecutar cypher_import_taxonomy.cypher primero.
// Los nodos Species deben existir antes de actualizar propiedades.
//
// INSTRUCCIONES:
// 1. Copiar wikidata_common_names.csv a la carpeta import/ de Neo4j
// 2. Asegurate de estar usando la base de datos LinNeo
//       :use LinNeo
// 3. Ejecutar este script
//

// -- CREAR INDEX en descripción (opcional, para búsqueda) --
CREATE INDEX species_common_names IF NOT EXISTS FOR (n:Species) ON (n.commonNames);


// -- LOAD CSV y actualizar Species con nombres comunes --
// 
// Este script:
// 1. Carga el CSV con estructura: species_key,common_name,language,wikidata_id,match_type
// 2. Agrupa nombres comunes por idioma
// 3. Actualiza los nodos Species con las propiedades
//

LOAD CSV WITH HEADERS FROM 'file:///wikidata_common_names.csv' AS row
WITH row.species_key AS species_key, 
     collect({name: row.common_name, language: row.language}) AS common_names,
     collect(DISTINCT row.language) AS languages
MATCH (s:Species {species_key: toInteger(species_key)})
SET s.commonNames = [cn.name | cn IN common_names],
    s.commonNamesLanguages = languages,
    s.wikidata_enriched = true
RETURN count(s) AS species_updated;


// -- VERIFICACIÓN --
//
// Ejecuta estos comandos para verificar los datos importados:
//

// Total de Species con nombres comunes
MATCH (s:Species)
WHERE s.commonNames IS NOT NULL AND size(s.commonNames) > 0
RETURN count(s) AS species_with_common_names;

// Ejemplo de Species enriquecida
MATCH (s:Species)
WHERE s.commonNames IS NOT NULL
RETURN s.scientific_name, s.commonNames, s.commonNamesLanguages
LIMIT 5;

// Estadísticas por idioma
MATCH (s:Species)
WHERE s.commonNamesLanguages IS NOT NULL
UNWIND s.commonNamesLanguages AS lang
WITH lang, count(DISTINCT s) AS count
RETURN lang, count
ORDER BY count DESC;
