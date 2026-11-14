"""Tests de endpoints contra un grafo fixture en el Neo4j desechable de ops/compose.yml.

    cd ops && docker compose --profile test up -d neo4j-test
    cd backend && python -m unittest discover -s tests

Borran todo en TEST_NEO4J_URI (por defecto bolt://localhost:7688, nunca el de la
app). Si ese Neo4j no responde, se saltan.
"""

import logging
import os
import sys
import unittest
from pathlib import Path

from neo4j import GraphDatabase

TEST_URI = os.getenv("TEST_NEO4J_URI", "bolt://localhost:7688")
os.environ["NEO4J_URI"] = TEST_URI  # antes de importar la app: gana sobre el .env
os.environ["NEO4J_PASSWORD"] = ""
os.environ["NEO4J_DATABASE"] = "neo4j"
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# El fixture omite propiedades opcionales; Neo4j avisa de cada una.
logging.getLogger("neo4j").setLevel(logging.ERROR)

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

SCHEMA = (Path(__file__).resolve().parents[2] / "ops" / "schema.cypher").read_text(encoding="utf-8")

# (taxonID, padre, rango, nombre, reino)
TREE = [
    (-1, None, "biota", "Biota", None),
    (-2, -1, "domain", "Eukaryota", None),
    (1, -2, "kingdom", "Animalia", "Animalia"),
    (44, 1, "phylum", "Chordata", "Animalia"),
    (359, 44, "class", "Mammalia", "Animalia"),
    (732, 359, "order", "Carnivora", "Animalia"),
    (9703, 732, "family", "Felidae", "Animalia"),
    (2435194, 9703, "genus", "Panthera", "Animalia"),
    (5219426, 2435194, "species", "Panthera onca", "Animalia"),
    (5219404, 2435194, "species", "Panthera leo", "Animalia"),
    (50, 44, "genus", "Jaguaria", "Animalia"),  # salto de rango: género bajo el filo
    (51, 50, "species", "Jaguaria minor", "Animalia"),
    (6, -2, "kingdom", "Plantae", "Plantae"),
    (2877951, 6, "genus", "Quercus", "Plantae"),
    (2878688, 2877951, "species", "Quercus robur", "Plantae"),
]
ONCA, LEO, MINOR, ROBUR = 5219426, 5219404, 51, 2878688
SPECIES = {
    ONCA: {"commonNames": ["jaguar", "yaguar", "onça-pintada"],
           "commonNamesLanguages": ["en", "es", "pt"], "iucn": "NT",
           "images": 2, "sounds": 1, "texts": ["summary", "reference"],
           "countries": {"MX": "native", "BR": "native"}},
    LEO: {"commonNames": ["lion"], "commonNamesLanguages": ["en"], "iucn": "VU",
          "images": 1, "sounds": 0, "texts": ["summary"], "countries": {"KE": "native"}},
    MINOR: {"commonNames": [], "commonNamesLanguages": [], "iucn": None,
            "images": 0, "sounds": 0, "texts": ["reference"], "countries": {}},
    ROBUR: {"commonNames": ["pedunculate oak"], "commonNamesLanguages": ["en"], "iucn": "LC",
            "images": 0, "sounds": 0, "texts": ["description"], "countries": {"FR": "native"}},
}
CONTINENT = {"MX": "North America", "BR": "South America", "KE": "Africa", "FR": "Europe"}


def species_under(key):
    children = {}
    for t, parent, *_ in TREE:
        children.setdefault(parent, []).append(t)
    stack, found = [key], []
    while stack:
        k = stack.pop()
        if k in SPECIES:
            found.append(k)
        stack.extend(children.get(k, []))
    return found


def aggregates(key):
    """Los mismos conteos que pipeline/aggregates.py, calculados a mano."""
    sp = [SPECIES[k] for k in species_under(key)]
    text = [s for s in sp if set(s["texts"]) - {"reference"}]
    agg = {
        "speciesCount": len(sp),
        "withImage": sum(s["images"] > 0 for s in sp),
        "withSound": sum(s["sounds"] > 0 for s in sp),
        "withText": len(text),
        "withCommonName": sum(bool(s["commonNames"]) for s in sp),
        "withContent": sum(bool(s["images"] or s["sounds"] or s in text or s["commonNames"])
                           for s in sp),
    }
    if key not in SPECIES:
        codes = sorted({s["iucn"] for s in sp if s["iucn"]})
        agg["countries"] = sorted({c for s in sp for c in s["countries"]})
        agg["iucnCodes"] = codes
        agg["iucnCounts"] = [sum(s["iucn"] == c for s in sp) for c in codes]
    return agg


def load_fixture(driver):
    driver.execute_query("MATCH (n) DETACH DELETE n")
    for statement in SCHEMA.split(";"):
        if statement.strip() and not statement.strip().startswith("//"):
            driver.execute_query(statement)
    for key, parent, rank, name, kingdom in TREE:
        props = {"taxonID": key, "name": name, "nameLower": name.lower(), "rank": rank,
                 "kingdom": kingdom, "scientific_name": name, **aggregates(key)}
        sp = SPECIES.get(key)
        if sp:
            props.update(commonNames=sp["commonNames"],
                         commonNamesLanguages=sp["commonNamesLanguages"],
                         conservation_overall_code=sp["iucn"])
        label = rank[0].upper() + rank[1:]
        driver.execute_query(f"CREATE (t:Taxon:{label}) SET t = $props", props=props)
        if parent is not None:
            driver.execute_query("""MATCH (p:Taxon {taxonID: $p}), (c:Taxon {taxonID: $c})
                                    CREATE (p)-[:HAS_CHILD]->(c)""", p=parent, c=key)
    for code, continent in CONTINENT.items():
        driver.execute_query("""MERGE (k:Continent {name: $continent})
                                CREATE (:Country {key: $code, name: $code})-[:PART_OF]->(k)""",
                             code=code, continent=continent)
    for key, sp in SPECIES.items():
        for i in range(sp["images"]):
            driver.execute_query("""MATCH (s:Taxon {taxonID: $k})
                CREATE (s)-[:HAS_MEDIA]->(:Media {media_type: 'image', url: $url})""",
                k=key, url=f"https://img/{key}/{i}.jpg")
        for i in range(sp["sounds"]):
            driver.execute_query("""MATCH (s:Taxon {taxonID: $k})
                CREATE (s)-[:HAS_MEDIA]->(:Media {media_type: 'sound', url: $url})""",
                k=key, url=f"https://snd/{key}/{i}.mp3")
        for kind in sp["texts"]:
            driver.execute_query("""MATCH (s:Taxon {taxonID: $k})
                CREATE (s)-[:HAS_DESCRIPTION]->(:Description {type: $type, text: 'texto', lang: 'en'})""",
                k=key, type=kind)
        for code, means in sp["countries"].items():
            driver.execute_query("""MATCH (s:Taxon {taxonID: $k}), (c:Country {key: $code})
                CREATE (s)-[:FOUND_IN {establishment_means: $means}]->(c)""",
                k=key, code=code, means=means)
    driver.execute_query("CALL db.awaitIndexes(60)")


def setUpModule():
    try:
        driver = GraphDatabase.driver(TEST_URI, auth=None)
        driver.verify_connectivity()
    except Exception as e:  # noqa: BLE001
        raise unittest.SkipTest(f"sin Neo4j de prueba en {TEST_URI}: {e}")
    with driver:
        load_fixture(driver)


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)
        cls.client.__enter__()  # corre el lifespan

    @classmethod
    def tearDownClass(cls):
        cls.client.__exit__(None, None, None)

    def get(self, path, status=200, **kw):
        r = self.client.get(path, **kw)
        self.assertEqual(r.status_code, status, r.text)
        return r.json() if status == 200 else r

    def test_stats(self):
        self.assertEqual(self.get("/stats"), {"total_species": 4, "with_description": 3,
                                              "with_image": 2, "with_sound": 1})

    def test_species_detail(self):
        s = self.get(f"/species/{ONCA}")
        self.assertEqual((s["key"], s["name"], s["conservation_overall_code"]),
                         (ONCA, "Panthera onca", "NT"))
        self.assertEqual([x["name"] for x in s["lineage"]],
                         ["Biota", "Eukaryota", "Animalia", "Chordata", "Mammalia", "Carnivora",
                          "Felidae", "Panthera", "Panthera onca"])
        self.assertEqual((len(s["media"]), len(s["descriptions"]), len(s["distribution"])),
                         (3, 2, 2))
        self.assertEqual(sorted(s["continents"]), ["North America", "South America"])
        rel = s["relatives"]
        self.assertEqual((rel["genus"]["key"], rel["family"]["key"]), (2435194, 9703))
        self.assertEqual([x["key"] for x in rel["sibling_species"]], [LEO])

    def test_species_404(self):
        self.get("/species/9703", status=404)  # una familia no es especie
        self.get("/species/999", status=404)

    def test_taxon_node(self):
        t = self.get("/taxon/2435194")
        self.assertEqual((t["child_rank"], t["species_count"], t["children_total"]),
                         ("species", 2, 2))
        self.assertEqual(t["countries"], ["BR", "KE", "MX"])
        onca = next(c for c in t["children"] if c["key"] == ONCA)
        self.assertEqual(onca["flags"], {"images": 2, "sounds": 1, "descriptions": 1,
                                         "etymology": 0, "countries": 2})
        self.assertIn(onca["image"], onca["images"])
        self.assertEqual(len(onca["images"]), 2)
        self.assertEqual(t["lineage"][-1]["key"], 2435194)
        self.get(f"/taxon/{ONCA}", status=404)

    def test_taxon_children_mixed_ranks_and_paging(self):
        # Rango más alto primero aunque el nombre ordene después (Mammalia > Jaguaria).
        t = self.get("/taxon/44?limit=1")
        self.assertEqual(t["children_total"], 2)
        self.assertEqual(t["child_ranks"], [{"rank": "class", "n": 1}, {"rank": "genus", "n": 1}])
        self.assertEqual([c["name"] for c in t["children"]], ["Mammalia"])
        t = self.get("/taxon/44?limit=1&offset=1")
        self.assertEqual([c["name"] for c in t["children"]], ["Jaguaria"])
        self.assertEqual(t["children"][0]["flags"]["species"], 1)

    def test_gallery(self):
        g = self.get("/taxon/-1/gallery")
        self.assertEqual([s["key"] for s in g["species"]], [LEO, ONCA])
        self.assertEqual(g["total"], 2)

    def test_sounds(self):
        s = self.get("/taxon/1/sounds")
        leaf = next(n for n in s["nodes"] if n["key"] == ONCA)
        self.assertEqual(leaf["sounds"], [f"https://snd/{ONCA}/0.mp3"])
        self.assertEqual(s["root_id"], "kingdom:1")
        self.assertEqual(len(s["edges"]), 6)  # Animalia ... Panthera onca
        self.assertEqual([c["key"] for c in s["children_pool"]["genus:2435194"]], [ONCA])

    def test_infographic(self):
        r = self.client.get("/taxon/-1/infographic")
        self.assertEqual(r.headers["cache-control"], "no-store")
        i = r.json()
        self.assertEqual(i["by_kingdom"], [{"kingdom": "Animalia", "count": 3},
                                           {"kingdom": "Plantae", "count": 1}])
        self.assertEqual(i["conservation"][0]["count"], 1)
        featured = {f["key"] for f in i["featured"]}
        self.assertTrue(featured <= {ONCA, LEO, ROBUR}, featured)  # minor no tiene contenido
        self.assertEqual(len(featured), len(i["featured"]))

    def test_random_descendants(self):
        for _ in range(5):
            keys = [s["key"] for s in self.get("/random/1?n=5")]
            self.assertTrue(set(keys) <= {ONCA, LEO}, keys)  # minor solo tiene referencia
            self.assertEqual(len(keys), len(set(keys)))

    def test_random_pool(self):
        self.assertEqual([s["key"] for s in self.get("/random/pool?kingdoms=Plantae&n=3")],
                         [ROBUR])

    def test_graph(self):
        g = self.get("/graph")
        self.assertEqual(g["center"], "biota:-1")
        ids = {n["id"] for n in g["nodes"]}
        self.assertTrue({"domain:-2", "kingdom:1", "kingdom:6"} <= ids)
        g = self.get("/graph/44")
        self.assertEqual(g["nodes"][1]["name"], "Mammalia")  # más especies primero

    def test_search_puts_exact_first(self):
        r = self.get("/search?q=jaguar")
        self.assertEqual(r["results"][0]["key"], ONCA)
        self.assertIn(MINOR, [x["key"] for x in r["results"]])
        r = self.get("/search?q=jaguar&offset=1&limit=1")
        self.assertEqual([x["key"] for x in r["results"]], [MINOR])

    def test_search_escapes_lucene(self):
        self.get('/search?q=panth*"era')

    def test_search_clades(self):
        r = self.get("/search/clades/species?q=onca")
        self.assertEqual(r["groups"][0]["items"][0]["key"], ONCA)
        r = self.get("/search/clades/species?q=on&mode=starts")
        self.assertEqual([i["key"] for g in r["groups"] for i in g["items"]], [ONCA])
        r = self.get("/search/clades/species?q=pan&mode=starts")
        self.assertEqual(r["groups"], [])
        self.assertIn("genus", self.get("/search/clades?q=panth")["groups"])
        self.get("/search/clades/tribe?q=onca", status=422)

    def test_filter(self):
        self.assertEqual([x["key"] for x in self.get("/filter?country=mx")["results"]], [ONCA])
        self.assertEqual(self.get("/filter?country=MX&kingdom=Plantae")["results"], [])
        self.get("/filter?kingdom=Plantae", status=422)  # sin país sería un escaneo completo

    def test_country(self):
        c = self.get("/map/country/mx")
        self.assertEqual((c["continent"], c["species_count"]), ("North America", 1))
        self.assertEqual(c["by_kingdom"], [{"kingdom": "Animalia", "n": 1}])
        self.get("/map/country/zz", status=404)

    def test_etag_short_circuits(self):
        r = self.client.get("/stats")
        etag = r.headers["etag"]
        self.assertIn("max-age", r.headers["cache-control"])
        r = self.client.get("/stats", headers={"If-None-Match": etag})
        self.assertEqual(r.status_code, 304)

    def test_cors_only_own_origin(self):
        ok = self.client.get("/stats", headers={"Origin": "http://localhost:5135"})
        self.assertEqual(ok.headers.get("access-control-allow-origin"), "http://localhost:5135")
        bad = self.client.get("/stats", headers={"Origin": "https://evil.example"})
        self.assertIsNone(bad.headers.get("access-control-allow-origin"))


if __name__ == "__main__":
    unittest.main()
