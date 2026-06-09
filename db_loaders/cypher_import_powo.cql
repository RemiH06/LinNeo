// ============================================================
// LinNeo -- Import POWO Plants (descripcion + habito)
// ============================================================
//
// - Descripcion: (Species)-[:HAS_DESCRIPTION]->(:Description {source_name:'powo'})
// - Habito: propiedad directa Species.habit (filtrable en el dashboard)
//
// REQUISITOS:
// 1. Nodos Species ya existen.
// 2. Copiar powo_plants.csv a la carpeta import/ de Neo4j.
// 3. :use LinNeo
//

CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);
CREATE INDEX description_source_url IF NOT EXISTS FOR (d:Description) ON (d.source_url);
CREATE INDEX species_habit IF NOT EXISTS FOR (s:Species) ON (s.habit);


// -- Habito: SET en Species (cuando hay valor) --
LOAD CSV WITH HEADERS FROM 'file:///powo_plants.csv' AS row
CALL {
  WITH row
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  SET s.habit = CASE WHEN row.habit IS NOT NULL AND row.habit <> '' THEN row.habit ELSE s.habit END
} IN TRANSACTIONS OF 1000 ROWS;


// -- Descripcion: crear Description solo cuando hay texto --
LOAD CSV WITH HEADERS FROM 'file:///powo_plants.csv' AS row
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

MATCH (:Species)-[:HAS_DESCRIPTION]->(d:Description {source_name: 'powo'})
RETURN count(d) AS descripciones_powo;

MATCH (s:Species) WHERE s.habit IS NOT NULL
RETURN count(s) AS especies_con_habito;

// Distribucion de habitos
MATCH (s:Species) WHERE s.habit IS NOT NULL
RETURN s.habit AS habito, count(*) AS total
ORDER BY total DESC
LIMIT 20;
