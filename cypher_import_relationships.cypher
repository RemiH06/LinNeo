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


// -- Species -[FOUND_IN]-> Country --
// Este bloque puede tardar varios minutos por el volumen de datos.

LOAD CSV WITH HEADERS FROM 'file:///species_country_rels.csv' AS row
CALL (row) {
  MATCH (s:Species {species_key: toInteger(row.species_key)})
  MATCH (c:Country {key: row.country_key})
  MERGE (s)-[:FOUND_IN]->(c)
} IN TRANSACTIONS OF 10000 ROWS;


// -- VERIFICACION --

MATCH ()-[r]->()
RETURN type(r) AS relacion, count(*) AS total
ORDER BY total DESC;