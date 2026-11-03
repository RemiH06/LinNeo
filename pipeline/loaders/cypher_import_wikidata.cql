// ============================================================
// LinNeo -- Import Wikidata Common Names
// ============================================================
//
// REQUISITO: los nodos Species ya existen.
// 1. Copiar wikidata_common_names.csv a la carpeta import/ de Neo4j
// 2. :use LinNeo
// 3. Ejecutar las sentencias una por una (el LOAD CSV va solo).
//

// -- INDICE --
CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);


// -- CARGA: agrupa nombres comunes por especie y los guarda como lista --
// (ejecutar esta sentencia sola)
LOAD CSV WITH HEADERS FROM 'file:///wikidata_common_names.csv' AS row
WITH row.species_key AS species_key,
     collect(DISTINCT row.common_name) AS common_names,
     collect(DISTINCT row.language) AS languages
MATCH (s:Species {species_key: toInteger(species_key)})
SET s.commonNames = common_names,
    s.commonNamesLanguages = languages,
    s.wikidata_enriched = true;


// ============================================================
// VERIFICACION
// ============================================================

MATCH (s:Species)
WHERE s.commonNames IS NOT NULL AND size(s.commonNames) > 0
RETURN count(s) AS species_con_nombres_comunes;

MATCH (s:Species)
WHERE s.commonNames IS NOT NULL
RETURN s.scientific_name, s.commonNames, s.commonNamesLanguages
LIMIT 5;
