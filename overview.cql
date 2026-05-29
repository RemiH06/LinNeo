// 1. Contar species con propiedades
MATCH (s:Species)
RETURN 
  count(*) as total_species,
  size(collect(DISTINCT s.name)) as especies_con_nombre,
  size(collect(DISTINCT s.scientific_name)) as con_scientific_name;

// 2. Ver propiedades de una species
MATCH (s:Species)
RETURN s LIMIT 1;

// 3. Contar FOUND_IN
MATCH ()-[r:FOUND_IN]->()
RETURN count(r) as total_found_in;

// 4. Ver si hay descripciones
MATCH (s:Species)
WHERE s.description IS NOT NULL
RETURN count(*) as con_descripcion
LIMIT 1;