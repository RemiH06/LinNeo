// ============================================================
// LinNeo -- Carga de extras del backbone GBIF
// ============================================================
//
// REQUISITO: nodos Species ya cargados (cypher_import_taxonomy.cypher).
// Copia a import/ los CSV generados por process_backbone_extras.py:
//   backbone_descriptions.csv, backbone_vernacular_es.csv, backbone_images.csv,
//   backbone_references.csv, backbone_types.csv
//
// Ejecuta cada bloque por separado. Los CALL IN TRANSACTIONS van SOLOS.
// Las especies que no existan como nodo se ignoran (MATCH no encuentra).

// -- Indices utiles --
CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);

// ── 1. Descripciones cientificas (description, diagnosis, biology, habitat, etymology, discussion, habit) ──
LOAD CSV WITH HEADERS FROM 'file:///backbone_descriptions.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  CREATE (d:Description {
    text: row.text,
    type: row.type,
    lang: row.language,
    source_name: row.source_name,
    origin: 'gbif_backbone'
  })
  MERGE (s)-[:HAS_DESCRIPTION]->(d)
} IN TRANSACTIONS OF 5000 ROWS;

// ── 2. Referencias bibliograficas (como Description type='reference') ──
LOAD CSV WITH HEADERS FROM 'file:///backbone_references.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  CREATE (d:Description {
    text: row.text,
    type: 'reference',
    source_name: row.source_name,
    origin: 'gbif_backbone'
  })
  MERGE (s)-[:HAS_DESCRIPTION]->(d)
} IN TRANSACTIONS OF 5000 ROWS;

// ── 3. Especimen tipo (como Description type='type_specimen') ──
LOAD CSV WITH HEADERS FROM 'file:///backbone_types.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  CREATE (d:Description {
    text: row.text,
    type: 'type_specimen',
    source_name: row.source_name,
    origin: 'gbif_backbone'
  })
  MERGE (s)-[:HAS_DESCRIPTION]->(d)
} IN TRANSACTIONS OF 5000 ROWS;

// ── 4. Imagenes (HAS_MEDIA) ──
LOAD CSV WITH HEADERS FROM 'file:///backbone_images.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  CREATE (m:Media {
    media_type: 'image',
    url: row.url,
    title: row.title,
    creator: row.creator,
    license: row.license,
    source_name: row.source_name,
    origin: 'gbif_backbone'
  })
  MERGE (s)-[:HAS_MEDIA]->(m)
} IN TRANSACTIONS OF 5000 ROWS;

// ── 5. Nombres comunes en espanol -> agregar a Species.commonNames (deduplicado) ──
LOAD CSV WITH HEADERS FROM 'file:///backbone_vernacular_es.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  WITH s, row,
       CASE WHEN s.commonNames IS NULL THEN [] ELSE s.commonNames END AS existentes
  WHERE NOT row.vernacular_name IN existentes
  SET s.commonNames = existentes + row.vernacular_name
} IN TRANSACTIONS OF 5000 ROWS;

// ── VERIFICACION ──
MATCH (:Species)-[:HAS_DESCRIPTION]->(d:Description {origin:'gbif_backbone'})
RETURN d.type AS tipo, count(*) AS total ORDER BY total DESC;

MATCH (:Species)-[:HAS_MEDIA]->(m:Media {origin:'gbif_backbone'})
RETURN count(m) AS imagenes_backbone;

// El leon con todo lo nuevo
MATCH (s:Species {species_key: 5219404})-[:HAS_DESCRIPTION]->(d:Description {origin:'gbif_backbone'})
RETURN d.type, left(d.text, 80) AS muestra;
