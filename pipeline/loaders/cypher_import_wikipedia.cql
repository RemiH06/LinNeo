// ============================================================
// LinNeo -- Import Wikipedia Descriptions
// ============================================================
//
// Modelo: (Species)-[:HAS_DESCRIPTION]->(:Description {...})
// Cada Description carga su propia procedencia (source_name, source_url).
// Mismo modelo servira para EOL y otras fuentes de descripcion.
//
// REQUISITOS:
// 1. Los nodos Species ya existen (cypher_import_taxonomy.cypher).
// 2. Copiar wikipedia_descriptions.csv a la carpeta import/ de Neo4j.
// 3. Estar en la base de datos LinNeo:   :use LinNeo
//

// -- INDICES (necesarios para que el MATCH/MERGE sea rapido) --
CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);
CREATE INDEX description_source_url IF NOT EXISTS FOR (d:Description) ON (d.source_url);


// -- CARGA (idempotente: se puede re-ejecutar sin duplicar) --
// MERGE por source_url (unico por articulo). CALL IN TRANSACTIONS
// procesa por lotes para no saturar memoria con cientos de miles de filas.

LOAD CSV WITH HEADERS FROM 'file:///wikipedia_descriptions.csv' AS row
CALL {
  WITH row
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

// Total de descripciones cargadas
MATCH (:Species)-[:HAS_DESCRIPTION]->(d:Description {source_name: 'wikipedia'})
RETURN count(d) AS descripciones_wikipedia;

// Especies con al menos una descripcion
MATCH (s:Species)-[:HAS_DESCRIPTION]->(:Description)
RETURN count(DISTINCT s) AS especies_con_descripcion;

// Ejemplo
MATCH (s:Species)-[:HAS_DESCRIPTION]->(d:Description)
RETURN s.scientific_name, d.lang, d.source_url, left(d.text, 160) AS preview
LIMIT 5;

// Distribucion por idioma
MATCH (:Species)-[:HAS_DESCRIPTION]->(d:Description)
RETURN d.lang AS idioma, count(*) AS total
ORDER BY total DESC;
