// ============================================================================
// QUERIES DE EJEMPLO PARA BASE DE DATOS DE BIODIVERSIDAD
// ============================================================================

// ----------------------------------------------------------------------------
// 1. ESPECIES DE PLANTAS EN LOS MONTES URALES Y REGIONES COLINDANTES
// ----------------------------------------------------------------------------

// Opción A: Si tienes coordenadas específicas de los Montes Urales
// Buscar especies dentro de un rango de lat/long
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Plantae'})
MATCH (s)-[r:FOUND_IN]->(c:Country)
WHERE r.latitude IS NOT NULL 
  AND r.longitude IS NOT NULL
  AND r.latitude >= 55.0 AND r.latitude <= 65.0  // Aprox Urales
  AND r.longitude >= 55.0 AND r.longitude <= 70.0
RETURN DISTINCT s.name as especie, 
       c.name as pais,
       r.latitude as lat, 
       r.longitude as lon
ORDER BY especie
LIMIT 100;

// Opción B: Si buscas por países específicos (Rusia, Kazajistán)
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Plantae'})
MATCH (s)-[:FOUND_IN]->(c:Country)
WHERE c.code IN ['RU', 'KZ']  // Rusia y Kazajistán
RETURN DISTINCT s.name as especie,
       collect(DISTINCT c.name) as paises
ORDER BY especie;

// ----------------------------------------------------------------------------
// 2. ESPECIES DE MAMÍFEROS EN TODA SUDAMÉRICA
// ----------------------------------------------------------------------------

// Mamíferos presentes en TODOS los países de Sudamérica
MATCH (s:Species)-[:BELONGS_TO]->(c:Class {name: 'Mammalia'})
MATCH (s)-[:FOUND_IN]->(country:Country)-[:PART_OF]->(cont:Continent {name: 'South America'})
WITH s, count(DISTINCT country) as num_paises, collect(DISTINCT country.name) as paises
WHERE num_paises >= 10  // Al menos en 10 países sudamericanos
RETURN s.name as especie,
       num_paises,
       paises
ORDER BY num_paises DESC;

// Mamíferos presentes en AL MENOS UN país de Sudamérica
MATCH (s:Species)-[:BELONGS_TO]->(c:Class {name: 'Mammalia'})
MATCH (s)-[:FOUND_IN]->(country:Country)-[:PART_OF]->(cont:Continent {name: 'South America'})
RETURN DISTINCT s.name as especie,
       collect(DISTINCT country.name) as paises_sudamericanos
ORDER BY especie;

// Contar cuántos mamíferos hay en cada país de Sudamérica
MATCH (s:Species)-[:BELONGS_TO]->(c:Class {name: 'Mammalia'})
MATCH (s)-[:FOUND_IN]->(country:Country)-[:PART_OF]->(cont:Continent {name: 'South America'})
RETURN country.name as pais,
       count(DISTINCT s) as num_mamiferos
ORDER BY num_mamiferos DESC;

// ----------------------------------------------------------------------------
// 3. ESPECIES DE BACTERIAS RELACIONADAS CON ENFERMEDADES HUMANAS POR CONTINENTE
// ----------------------------------------------------------------------------

// NOTA: Esto requiere que hayas agregado atributos de "patógeno" a las especies
// Asumiendo que tienes una propiedad s.is_pathogen o s.causes_disease

// Opción A: Con propiedad booleana
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Bacteria'})
WHERE s.is_pathogen = true OR s.causes_disease = true
MATCH (s)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
RETURN cont.name as continente,
       count(DISTINCT s) as num_bacterias_patogenas
ORDER BY num_bacterias_patogenas DESC;

// Opción B: Si tienes una lista de especies patógenas conocidas
WITH ['Escherichia coli', 'Salmonella enterica', 'Mycobacterium tuberculosis', 
      'Staphylococcus aureus', 'Streptococcus pneumoniae'] as patogenos
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Bacteria'})
WHERE s.name IN patogenos
MATCH (s)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
RETURN cont.name as continente,
       collect(DISTINCT s.name) as bacterias_patogenas,
       count(DISTINCT s) as cantidad
ORDER BY cantidad DESC;

// Distribución global de una bacteria patógena específica
MATCH (s:Species {name: 'Escherichia coli'})-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
RETURN cont.name as continente,
       collect(DISTINCT c.name) as paises,
       count(DISTINCT c) as num_paises
ORDER BY num_paises DESC;

// ----------------------------------------------------------------------------
// 4. PORCENTAJE DE HONGOS VENENOSOS EN OCEANÍA
// ----------------------------------------------------------------------------

// NOTA: Esto requiere que hayas agregado atributos de "venenoso" a las especies
// Asumiendo que tienes una propiedad s.is_poisonous o s.toxicity_level

// Total de hongos en Oceanía
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Fungi'})
MATCH (s)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent {name: 'Oceania'})
WITH count(DISTINCT s) as total_hongos

// Hongos venenosos en Oceanía
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Fungi'})
WHERE s.is_poisonous = true
MATCH (s)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent {name: 'Oceania'})
WITH total_hongos, count(DISTINCT s) as hongos_venenosos

RETURN total_hongos,
       hongos_venenosos,
       round(100.0 * hongos_venenosos / total_hongos, 2) as porcentaje_venenosos;

// Desglose por país en Oceanía
MATCH (s:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Fungi'})
MATCH (s)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent {name: 'Oceania'})
WITH c, count(DISTINCT s) as total
MATCH (sv:Species)-[:BELONGS_TO]->(k:Kingdom {name: 'Fungi'})
WHERE sv.is_poisonous = true
MATCH (sv)-[:FOUND_IN]->(c)
WITH c, total, count(DISTINCT sv) as venenosos
RETURN c.name as pais,
       total as total_hongos,
       venenosos as hongos_venenosos,
       round(100.0 * venenosos / total, 2) as porcentaje
ORDER BY porcentaje DESC;

// ----------------------------------------------------------------------------
// QUERIES ADICIONALES ÚTILES
// ----------------------------------------------------------------------------

// 5. Especies endémicas de un país (solo en ese país)
MATCH (s:Species)-[:FOUND_IN]->(c:Country {code: 'AU'})  // Australia
WITH s, count(*) as num_paises
WHERE num_paises = 1
MATCH (s)-[:BELONGS_TO*]->(k:Kingdom)
RETURN s.name as especie_endemica,
       k.name as reino
LIMIT 50;

// 6. Biodiversidad por continente (número de especies únicas)
MATCH (s:Species)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
WITH cont, count(DISTINCT s) as num_especies
RETURN cont.name as continente,
       num_especies
ORDER BY num_especies DESC;

// 7. Especies que están en múltiples continentes (cosmopolitas)
MATCH (s:Species)-[:FOUND_IN]->(:Country)-[:PART_OF]->(cont:Continent)
WITH s, collect(DISTINCT cont.name) as continentes, count(DISTINCT cont) as num_continentes
WHERE num_continentes >= 3
RETURN s.name as especie_cosmopolita,
       continentes,
       num_continentes
ORDER BY num_continentes DESC
LIMIT 50;

// 8. Jerarquía taxonómica completa de una especie
MATCH path = (s:Species {name: 'Homo sapiens'})-[:BELONGS_TO*]->(k:Kingdom)
RETURN [n in nodes(path) | labels(n)[0] + ': ' + n.name] as jerarquia_taxonomica;

// 9. Familias con más especies en un continente
MATCH (s:Species)-[:BELONGS_TO]->(f:Family)
MATCH (s)-[:FOUND_IN]->(:Country)-[:PART_OF]->(cont:Continent {name: 'Africa'})
WITH f, count(DISTINCT s) as num_especies
RETURN f.name as familia,
       num_especies
ORDER BY num_especies DESC
LIMIT 20;

// 10. Especies invasoras (presentes en continentes no nativos)
// NOTA: Esto requiere datos adicionales sobre rangos nativos vs introducidos
MATCH (s:Species)-[:FOUND_IN]->(c:Country)-[:PART_OF]->(cont:Continent)
WHERE s.native_continent IS NOT NULL 
  AND cont.name <> s.native_continent
RETURN s.name as especie_invasora,
       s.native_continent as continente_nativo,
       collect(DISTINCT cont.name) as continentes_invadidos
LIMIT 50;

// ----------------------------------------------------------------------------
// QUERIES DE ANÁLISIS Y ESTADÍSTICAS
// ----------------------------------------------------------------------------

// 11. Resumen de la base de datos
MATCH (k:Kingdom)
OPTIONAL MATCH (k)<-[:BELONGS_TO*]-(s:Species)
RETURN k.name as reino,
       count(DISTINCT s) as num_especies
ORDER BY num_especies DESC;

// 12. Países con mayor biodiversidad
MATCH (s:Species)-[:FOUND_IN]->(c:Country)
WITH c, count(DISTINCT s) as biodiversidad
MATCH (c)-[:PART_OF]->(cont:Continent)
RETURN c.name as pais,
       cont.name as continente,
       biodiversidad
ORDER BY biodiversidad DESC
LIMIT 20;

// 13. Especies por clase taxonómica
MATCH (s:Species)-[:BELONGS_TO]->(c:Class)
WITH c, count(DISTINCT s) as num_especies
RETURN c.name as clase,
       num_especies
ORDER BY num_especies DESC
LIMIT 20;

// 14. Hotspots de biodiversidad (regiones con alta concentración de especies)
MATCH (s:Species)-[r:FOUND_IN]->(c:Country)
WHERE r.latitude IS NOT NULL AND r.longitude IS NOT NULL
WITH round(r.latitude, 0) as lat_bucket, 
     round(r.longitude, 0) as lon_bucket,
     count(DISTINCT s) as especies_en_area
WHERE especies_en_area > 10
RETURN lat_bucket, 
       lon_bucket, 
       especies_en_area
ORDER BY especies_en_area DESC
LIMIT 50;

// ============================================================================
// NOTAS IMPORTANTES:
// ============================================================================
// 
// 1. Muchas de estas queries asumen atributos adicionales como:
//    - s.is_poisonous, s.is_pathogen, s.causes_disease
//    - s.native_continent, s.toxicity_level
//    
//    Estos atributos NO vienen en GBIF y deben ser agregados manualmente
//    desde otras fuentes especializadas.
//
// 2. Para queries geográficas precisas (como "Montes Urales"), necesitarás:
//    - Definir polígonos geográficos
//    - Usar funciones espaciales de Neo4j
//    - O trabajar con coordenadas lat/long directamente
//
// 3. Para optimizar el rendimiento con grandes volúmenes de datos:
//    - Asegúrate de tener índices en propiedades frecuentemente consultadas
//    - Usa PROFILE o EXPLAIN para analizar queries lentas
//    - Considera materializar vistas para queries complejas frecuentes
//
// 4. Para agregar atributos adicionales (venenoso, patógeno, etc.):
//    - Consulta bases de datos especializadas (WHO, IUCN, toxicology DBs)
//    - Usa web scraping de fuentes confiables
//    - Integra literatura científica mediante NLP
//
// ============================================================================
MATCH (s:Species)-[:BELONGS_TO]->(c:Class)
WITH c, count(DISTINCT s) as num_especies
RETURN c.name as clase,
       num_especies
ORDER BY num_especies DESC
LIMIT 20;