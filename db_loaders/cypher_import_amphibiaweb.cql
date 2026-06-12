// ============================================================
// LinNeo -- Import AmphibiaWeb Descriptions
// ============================================================
//
// Mismo modelo: (Species)-[:HAS_DESCRIPTION]->(:Description {source_name:'amphibiaweb'})
//
// REQUISITOS:
// 1. Nodos Species ya existen.
// 2. Copiar amphibiaweb_data.csv a la carpeta import/ de Neo4j.
// 3. :use LinNeo
//

CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);
CREATE INDEX description_source_url IF NOT EXISTS FOR (d:Description) ON (d.source_url);


LOAD CSV WITH HEADERS FROM 'file:///amphibiaweb_data.csv' AS row
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

MATCH (:Species)-[:HAS_DESCRIPTION]->(d:Description {source_name: 'amphibiaweb'})
RETURN count(d) AS descripciones_amphibiaweb;

MATCH (s:Species)-[:HAS_DESCRIPTION]->(d:Description {source_name: 'amphibiaweb'})
RETURN s.scientific_name, left(d.text, 140) AS preview
LIMIT 5;
