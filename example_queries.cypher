// ============================================================
// LinNeo -- Queries de ejemplo
// ============================================================
// Ejecuta cada bloque por separado en Neo4j Browser.


// -- TAXONOMIA --

// Cuantos nodos hay de cada rango taxonomico
MATCH (n)
RETURN labels(n)[0] AS rango, count(*) AS total
ORDER BY total DESC;

// Ver la jerarquia completa de una especie especifica
MATCH path = (k:Kingdom)-[:HAS_CHILD*]->(s:Species {name: "Homo sapiens"})
RETURN path;


// -- GEOGRAFIA --

// Cuantas especies hay por continente
MATCH (s:Species)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
RETURN cont.name AS continente, count(DISTINCT s) AS especies
ORDER BY especies DESC;

// Que paises comparten mas especies (pares de paises con fauna/flora comun)
MATCH (s:Species)-[:FOUND_IN]->(c1:Country)
MATCH (s)-[:FOUND_IN]->(c2:Country)
WHERE c1.key < c2.key
RETURN c1.key AS pais_1, c2.key AS pais_2, count(DISTINCT s) AS especies_compartidas
ORDER BY especies_compartidas DESC
LIMIT 20;


// -- DISTRIBUCION POR REINO --

// Cuantas especies de cada reino hay en cada continente
MATCH (s:Species)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
RETURN cont.name AS continente, s.kingdom AS reino, count(DISTINCT s) AS especies
ORDER BY continente, especies DESC;

// Reinos con presencia en mas paises
MATCH (s:Species)-[:FOUND_IN]->(c:Country)
RETURN s.kingdom AS reino, count(DISTINCT c) AS paises
ORDER BY paises DESC;


// -- ENDEMISMO --

// Especies que solo se encuentran en un unico pais (endemicas)
MATCH (s:Species)-[:FOUND_IN]->(c:Country)
WITH s, count(DISTINCT c) AS num_paises
WHERE num_paises = 1
MATCH (s)-[:FOUND_IN]->(c:Country)
RETURN c.key AS pais, count(s) AS especies_endemicas
ORDER BY especies_endemicas DESC
LIMIT 20;

// Especies endemicas de un pais especifico con su taxonomia completa
MATCH (s:Species)-[:FOUND_IN]->(c:Country {key: "MX"})
WITH s, count(DISTINCT c) AS num_paises
WHERE num_paises = 1
MATCH path = (k:Kingdom)-[:HAS_CHILD*]->(s)
RETURN s.name AS especie, s.kingdom AS reino
ORDER BY reino, especie
LIMIT 50;


// -- FAMILIAS Y GENEROS --

// Familias con mas especies en un continente dado
MATCH (f:Family)-[:HAS_CHILD]->(g:Genus)-[:HAS_CHILD]->(s:Species)
MATCH (s)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent {name: "South America"})
RETURN f.name AS familia, count(DISTINCT s) AS especies
ORDER BY especies DESC
LIMIT 20;

// Generos presentes en todos los continentes (cosmopolitas)
MATCH (cont:Continent)
WITH count(cont) AS total_continentes
MATCH (g:Genus)-[:HAS_CHILD]->(s:Species)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
WITH g, count(DISTINCT cont) AS continentes_presentes, total_continentes
WHERE continentes_presentes = total_continentes
RETURN g.name AS genero, continentes_presentes
ORDER BY genero;
