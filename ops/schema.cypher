CREATE CONSTRAINT taxon_id IF NOT EXISTS FOR (t:Taxon) REQUIRE t.taxonID IS UNIQUE;
CREATE CONSTRAINT country_key IF NOT EXISTS FOR (c:Country) REQUIRE c.key IS UNIQUE;
CREATE FULLTEXT INDEX taxon_search IF NOT EXISTS
  FOR (t:Taxon) ON EACH [t.name, t.synonyms, t.commonNames];
CALL db.awaitIndexes(1800);
