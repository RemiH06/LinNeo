// ============================================================
// LinNeo -- Import FishBase (descripcion + habitat)
// ============================================================
//
// - Descripcion: (Species)-[:HAS_DESCRIPTION]->(:Description {source_name:'fishbase'})
// - Habitat: propiedad directa Species.habit (ej: "freshwater; demersal")
//
// REQUISITOS:
// 1. Nodos Species ya existen.
// 2. Copiar fishbase_fish.csv a la carpeta import/ de Neo4j.
// 3. :use LinNeo
//

CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);
CREATE INDEX description_source_url IF NOT EXISTS FOR (d:Description) ON (d.source_url);
CREATE INDEX species_habit IF NOT EXISTS FOR (s:Species) ON (s.habit);


// -- Habitat: SET en Species --
LOAD CSV WITH HEADERS FROM 'file:///fishbase_fish.csv' AS row
CALL {
  WITH row
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  SET s.habit = CASE WHEN row.habit IS NOT NULL AND row.habit <> '' THEN row.habit ELSE s.habit END
} IN TRANSACTIONS OF 1000 ROWS;


// -- Descripcion: crear Description solo cuando hay texto --
LOAD CSV WITH HEADERS FROM 'file:///fishbase_fish.csv' AS row
CALL {
  WITH row
  WITH row WHERE row.text IS NOT NULL AND row.text <> ''
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  MERGE (d:Description {source_url: row.source_url})
  SET d.text          = row.text,
      d.lang          = row.lang,
      d.source_name   = row.source_name,
      d.accessed_date = row.accessed_date
  MERGE (s)-[:HAS_DESCRIPTION]->(d)
} IN TRANSACTIONS OF 1000 ROWS;


// ============================================================
// VERIFICACION
// ============================================================

MATCH (:Species)-[:HAS_DESCRIPTION]->(d:Description {source_name: 'fishbase'})
RETURN count(d) AS descripciones_fishbase;

MATCH (s:Species) WHERE s.habit CONTAINS 'water' OR s.habit CONTAINS 'marine'
RETURN count(s) AS peces_con_habitat;

MATCH (s:Species)-[:HAS_DESCRIPTION]->(d:Description {source_name: 'fishbase'})
RETURN s.scientific_name, s.habit, left(d.text, 120) AS preview
LIMIT 5;
