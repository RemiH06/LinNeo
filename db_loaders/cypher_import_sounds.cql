// ============================================================
// LinNeo -- Import Xeno-canto Sounds
// ============================================================
//
// Modelo: (Species)-[:HAS_MEDIA]->(:Media {media_type:"sound", ...})
// Mismo modelo que imagenes; solo cambia media_type y el CSV de entrada.
//
// REQUISITOS:
// 1. Nodos Species ya existen.
// 2. Copiar xeno_canto_sounds.csv a la carpeta import/ de Neo4j.
// 3. :use LinNeo
//

// -- INDICES --
CREATE INDEX species_species_key IF NOT EXISTS FOR (s:Species) ON (s.species_key);
CREATE INDEX media_url IF NOT EXISTS FOR (m:Media) ON (m.url);


// -- CARGA (idempotente: MERGE por url) --
LOAD CSV WITH HEADERS FROM 'file:///xeno_canto_sounds.csv' AS row
CALL {
  WITH row
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  MERGE (m:Media {url: row.url})
  SET m.media_type    = row.media_type,
      m.source_name   = row.source_name,
      m.source_url    = row.source_url,
      m.license       = row.license,
      m.accessed_date = row.accessed_date
  MERGE (s)-[:HAS_MEDIA]->(m)
} IN TRANSACTIONS OF 1000 ROWS;


// ============================================================
// VERIFICACION
// ============================================================

MATCH (:Species)-[:HAS_MEDIA]->(m:Media {media_type: 'sound'})
RETURN count(m) AS total_sonidos;

MATCH (s:Species)-[:HAS_MEDIA]->(:Media {media_type: 'sound'})
RETURN count(DISTINCT s) AS especies_con_sonido;

MATCH (s:Species)-[:HAS_MEDIA]->(m:Media {media_type: 'sound'})
RETURN s.scientific_name, m.url, m.license
LIMIT 5;
