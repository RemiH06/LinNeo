CREATE CONSTRAINT taxon_id IF NOT EXISTS FOR (t:Taxon) REQUIRE t.taxonID IS UNIQUE;
CREATE CONSTRAINT country_key IF NOT EXISTS FOR (c:Country) REQUIRE c.key IS UNIQUE;
CREATE FULLTEXT INDEX taxon_search IF NOT EXISTS
  FOR (t:Taxon) ON EACH [t.name, t.synonyms, t.commonNames];
// Búsqueda por subcadena en un rango (/search/clades): índice de trigramas.
CREATE TEXT INDEX taxon_name_lower IF NOT EXISTS FOR (t:Taxon) ON (t.nameLower);
CALL db.awaitIndexes(1800);
