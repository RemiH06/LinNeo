// ============================================================
// LinNeo -- Import nodos taxonomicos (Kingdom a Species)
// ============================================================
//
// INSTRUCCIONES:
// 1. Copia los CSVs a la carpeta import/ de tu instancia Neo4j
// 2. Asegurate de estar usando la base de datos LinNeo
//       :use LinNeo
// 3. Ejecuta primero el bloque de constraints
// 4. Luego ejecuta cada bloque LOAD CSV por separado
//

// -- CONSTRAINTS (ejecutar primero, una sola vez) --

CREATE CONSTRAINT kingdom_key IF NOT EXISTS FOR (n:Kingdom) REQUIRE n.kingdom_key IS UNIQUE;
CREATE CONSTRAINT phylum_key IF NOT EXISTS FOR (n:Phylum) REQUIRE n.phylum_key IS UNIQUE;
CREATE CONSTRAINT class_key IF NOT EXISTS FOR (n:Class) REQUIRE n.class_key IS UNIQUE;
CREATE CONSTRAINT order_key IF NOT EXISTS FOR (n:Order) REQUIRE n.order_key IS UNIQUE;
CREATE CONSTRAINT family_key IF NOT EXISTS FOR (n:Family) REQUIRE n.family_key IS UNIQUE;
CREATE CONSTRAINT genus_key IF NOT EXISTS FOR (n:Genus) REQUIRE n.genus_key IS UNIQUE;
CREATE CONSTRAINT species_key IF NOT EXISTS FOR (n:Species) REQUIRE n.species_key IS UNIQUE;


// -- LOAD CSV Kingdom a Genus (ejecutar cada bloque por separado) --

// Kingdom
LOAD CSV WITH HEADERS FROM 'file:///kingdom_nodes.csv' AS row
CALL (row) {
  MERGE (n:Kingdom {kingdom_key: toInteger(row.key)})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// Phylum
LOAD CSV WITH HEADERS FROM 'file:///phylum_nodes.csv' AS row
CALL (row) {
  MERGE (n:Phylum {phylum_key: toInteger(row.key)})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// Class
LOAD CSV WITH HEADERS FROM 'file:///class_nodes.csv' AS row
CALL (row) {
  MERGE (n:Class {class_key: toInteger(row.key)})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// Order
LOAD CSV WITH HEADERS FROM 'file:///order_nodes.csv' AS row
CALL (row) {
  MERGE (n:Order {order_key: toInteger(row.key)})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// Family
LOAD CSV WITH HEADERS FROM 'file:///family_nodes.csv' AS row
CALL (row) {
  MERGE (n:Family {family_key: toInteger(row.key)})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// Genus
LOAD CSV WITH HEADERS FROM 'file:///genus_nodes.csv' AS row
CALL (row) {
  MERGE (n:Genus {genus_key: toInteger(row.key)})
  SET n.name = row.name
} IN TRANSACTIONS OF 10000 ROWS;

// -- LOAD CSV Species --
// Solo especies accepted. Incluye kingdom_key para trazabilidad.

LOAD CSV WITH HEADERS FROM 'file:///species_nodes.csv' AS row
CALL (row) {
  MERGE (n:Species {species_key: toInteger(row.key)})
  SET n.name            = row.name
  SET n.scientific_name = row.scientific_name
  SET n.kingdom         = row.kingdom
  SET n.kingdom_key     = toInteger(row.kingdom_key)
} IN TRANSACTIONS OF 10000 ROWS;


// -- VERIFICACION --

MATCH (n)
RETURN labels(n)[0] AS label, count(*) AS total
ORDER BY total DESC;