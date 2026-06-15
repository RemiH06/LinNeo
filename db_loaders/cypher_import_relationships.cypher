// ============================================================
// LinNeo -- Import geografia y relaciones
// ============================================================
//
// REQUISITO: Ejecutar cypher_import_taxonomy.cypher primero.
// Los nodos taxonomicos deben existir antes de crear las relaciones.
//
// INSTRUCCIONES:
// 1. Asegurate de estar usando la base de datos LinNeo
//       :use LinNeo
// 2. Ejecuta cada bloque por separado en el orden indicado
//
// NOTA: la geografia ahora sale del Distribution.tsv del backbone (242 paises)
// con datos enriquecidos: establishment_means, occurrence_status y conservacion
// por pais en la relacion FOUND_IN, mas el estado general en Species.

// -- (OPCIONAL) limpiar la distribucion vieja de 93 paises antes de recargar --
// Ejecuta este bloque SOLO si ya tenias cargada la version anterior.
MATCH (:Species)-[r:FOUND_IN]->(:Country) CALL (r) { DELETE r } IN TRANSACTIONS OF 10000 ROWS;
MATCH (c:Country)-[r:PART_OF]->(:Continent) DELETE r;
MATCH (c:Country) DELETE c;
MATCH (k:Continent) DELETE k;

// -- CONSTRAINTS geograficos (ejecutar primero) --
CREATE CONSTRAINT continent_name IF NOT EXISTS FOR (n:Continent) REQUIRE n.name IS UNIQUE;
CREATE CONSTRAINT country_key IF NOT EXISTS FOR (n:Country) REQUIRE n.key IS UNIQUE;

// -- LOAD CSV Continent --
LOAD CSV WITH HEADERS FROM 'file:///continent_nodes.csv' AS row
CALL (row) {
  MERGE (n:Continent {name: row.name})
} IN TRANSACTIONS OF 10000 ROWS;

// -- LOAD CSV Country --
LOAD CSV WITH HEADERS FROM 'file:///country_nodes.csv' AS row
CALL (row) {
  MERGE (n:Country {key: row.key})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// -- Country -[PART_OF]-> Continent --
LOAD CSV WITH HEADERS FROM 'file:///country_continent_rels.csv' AS row
CALL (row) {
  MATCH (country:Country {key: row.country_key})
  MATCH (cont:Continent {name: row.continent_name})
  MERGE (country)-[:PART_OF]->(cont)
} IN TRANSACTIONS OF 10000 ROWS;

// -- Relaciones taxonomicas (Kingdom a Species) --
// Kingdom -[HAS_CHILD]-> Phylum
LOAD CSV WITH HEADERS FROM 'file:///kingdom_phylum_rels.csv' AS row
CALL (row) {
  MATCH (parent:Kingdom {kingdom_key: toInteger(row.parent_key)})
  MATCH (child:Phylum {phylum_key: toInteger(row.child_key)})
  MERGE (parent)-[:HAS_CHILD]->(child)
} IN TRANSACTIONS OF 10000 ROWS;
// Phylum -[HAS_CHILD]-> Class
LOAD CSV WITH HEADERS FROM 'file:///phylum_class_rels.csv' AS row
CALL (row) {
  MATCH (parent:Phylum {phylum_key: toInteger(row.parent_key)})
  MATCH (child:Class {class_key: toInteger(row.child_key)})
  MERGE (parent)-[:HAS_CHILD]->(child)
} IN TRANSACTIONS OF 10000 ROWS;
// Class -[HAS_CHILD]-> Order
LOAD CSV WITH HEADERS FROM 'file:///class_order_rels.csv' AS row
CALL (row) {
  MATCH (parent:Class {class_key: toInteger(row.parent_key)})
  MATCH (child:Order {order_key: toInteger(row.child_key)})
  MERGE (parent)-[:HAS_CHILD]->(child)
} IN TRANSACTIONS OF 10000 ROWS;
// Order -[HAS_CHILD]-> Family
LOAD CSV WITH HEADERS FROM 'file:///order_family_rels.csv' AS row
CALL (row) {
  MATCH (parent:Order {order_key: toInteger(row.parent_key)})
  MATCH (child:Family {family_key: toInteger(row.child_key)})
  MERGE (parent)-[:HAS_CHILD]->(child)
} IN TRANSACTIONS OF 10000 ROWS;
// Family -[HAS_CHILD]-> Genus
LOAD CSV WITH HEADERS FROM 'file:///family_genus_rels.csv' AS row
CALL (row) {
  MATCH (parent:Family {family_key: toInteger(row.parent_key)})
  MATCH (child:Genus {genus_key: toInteger(row.child_key)})
  MERGE (parent)-[:HAS_CHILD]->(child)
} IN TRANSACTIONS OF 10000 ROWS;
// Genus -[HAS_CHILD]-> Species
LOAD CSV WITH HEADERS FROM 'file:///genus_species_rels.csv' AS row
CALL (row) {
  MATCH (parent:Genus {genus_key: toInteger(row.parent_key)})
  MATCH (child:Species {species_key: toInteger(row.child_key)})
  MERGE (parent)-[:HAS_CHILD]->(child)
} IN TRANSACTIONS OF 10000 ROWS;

// -- Species -[FOUND_IN]-> Country (relacion enriquecida) --
// Este bloque puede tardar varios minutos por el volumen de datos.
LOAD CSV WITH HEADERS FROM 'file:///species_country_rels.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  MATCH (c:Country {key: row.country_key})
  MERGE (s)-[f:FOUND_IN]->(c)
  SET f.establishment_means = row.establishment_means,
      f.occurrence_status   = row.occurrence_status,
      f.conservation_status = row.conservation_status,
      f.conservation_code   = row.conservation_code
} IN TRANSACTIONS OF 10000 ROWS;

// -- Estado de conservacion general (peor caso) en Species --
LOAD CSV WITH HEADERS FROM 'file:///species_conservation.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  SET s.conservation_overall      = row.conservation_overall,
      s.conservation_overall_code = row.conservation_overall_code
} IN TRANSACTIONS OF 10000 ROWS;

// -- VERIFICACION --
MATCH (c:Country) RETURN count(c) AS total_paises;                 // esperado: 242
MATCH ()-[r]->()
RETURN type(r) AS relacion, count(*) AS total
ORDER BY total DESC;

// El leon: distribucion con establishment_means
MATCH (s:Species {species_key: 5219404})-[f:FOUND_IN]->(c:Country)
RETURN c.key, f.establishment_means, f.conservation_code
ORDER BY c.key;

// Distribucion de tipos de presencia
MATCH (:Species)-[f:FOUND_IN]->(:Country)
RETURN f.establishment_means AS tipo, count(*) AS total
ORDER BY total DESC;
