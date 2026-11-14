"""Conteos por taxón sobre sus especies descendientes.

    python -m unittest discover -s pipeline/tests
"""
import sys
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import aggregates  # noqa: E402

TREE = [
    (-1, None, "biota"),
    (1, -1, "kingdom"),
    (10, 1, "phylum"),
    (100, 10, "order"),       # salto de rango: cuelga directo del filo
    (1000, 100, "genus"),
    (2000, 100, "genus"),     # género sin especies
    (11, 1000, "species"),
    (12, 1000, "species"),
    (13, 10, "species"),      # especie colgada del filo
]
CONTENT = [(11, "image"), (11, "sound"), (12, "text"), (12, "text"), (13, "common"),
           (1000, "image")]   # el contenido propio de un género no cuenta como especie
GEO = [(11, "MX"), (12, "MX"), (13, "BR"), (1000, "AR")]
IUCN = [(11, "EN"), (12, "LC"), (13, "EN"), (1000, "CR")]


def run():
    con = duckdb.connect()
    con.execute("CREATE TABLE tree(taxonID BIGINT, parentID BIGINT, rank VARCHAR)")
    con.executemany("INSERT INTO tree VALUES (?, ?, ?)", TREE)
    con.execute("CREATE TABLE content(taxonID BIGINT, kind VARCHAR)")
    con.executemany("INSERT INTO content VALUES (?, ?)", CONTENT)
    con.execute("CREATE TABLE geo(taxonID BIGINT, country_key VARCHAR)")
    con.executemany("INSERT INTO geo VALUES (?, ?)", GEO)
    con.execute("CREATE TABLE iucn(taxonID BIGINT, code VARCHAR)")
    con.executemany("INSERT INTO iucn VALUES (?, ?)", IUCN)
    aggregates.build(con, "tree", "content", "geo", "iucn")
    cols = [d[0] for d in con.execute("SELECT * FROM agg").description]
    return {row[0]: dict(zip(cols, row)) for row in con.execute("SELECT * FROM agg").fetchall()}


class TestAggregates(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.agg = run()

    def test_counts_roll_up(self):
        root = self.agg[-1]
        self.assertEqual(
            (root["speciesCount"], root["withImage"], root["withSound"], root["withText"],
             root["withCommonName"], root["withContent"]),
            (3, 1, 1, 1, 1, 3))
        self.assertEqual(self.agg[100]["speciesCount"], 2)

    def test_parent_is_sum_of_children(self):
        self.assertEqual(self.agg[10]["withContent"],
                         self.agg[100]["withContent"] + self.agg[13]["withContent"])

    def test_species_count_themselves(self):
        self.assertEqual((self.agg[12]["speciesCount"], self.agg[12]["withText"],
                          self.agg[12]["withImage"]), (1, 1, 0))

    def test_taxon_without_species_is_absent(self):
        self.assertNotIn(2000, self.agg)

    def test_countries_and_iucn(self):
        self.assertEqual(self.agg[10]["countries"], ["AR", "BR", "MX"])
        self.assertEqual(self.agg[1000]["countries"], ["AR", "MX"])
        self.assertEqual((self.agg[10]["iucnCodes"], self.agg[10]["iucnCounts"]),
                         (["EN", "LC"], [2, 1]))
        self.assertIsNone(self.agg[11]["countries"])
        self.assertIsNone(self.agg[11]["iucnCodes"])


if __name__ == "__main__":
    unittest.main()
