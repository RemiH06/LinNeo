"""Cada invariante debe fallar ante el dato que dice detectar.

    python -m unittest discover -s pipeline/tests
"""
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from invariants import check  # noqa: E402

GOOD = [
    (-1, None, "biota", "Biota", "synthetic", None),
    (-2, -1, "domain", "Eukaryota", "synthetic", None),
    (1, -2, "kingdom", "Animalia", "accepted", "Animalia"),
    (10, 1, "phylum", "Mollusca", "accepted", "Animalia"),
    (20, 1, "phylum", "Chordata", "accepted", "Animalia"),
    (4257, 10, "family", "Cepolidae", "accepted", "Animalia"),
    (6755596, 20, "family", "Cepolidae", "doubtful", "Animalia"),
    (100, 4257, "genus", "Cepolaea", "accepted", "Animalia"),
    (200, 100, "species", "Cepolaea a", "accepted", "Animalia"),
    (300, 6755596, "species", "Cepola b", "accepted", "Animalia"),
]


def run(rows):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp, "taxa.parquet").as_posix()
        con = duckdb.connect()
        con.execute("""CREATE TABLE t(taxonID BIGINT, parentID BIGINT, rank VARCHAR,
                       name VARCHAR, status VARCHAR, kingdom VARCHAR)""")
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", rows)
        con.execute(f"COPY t TO '{path}' (FORMAT parquet)")
        failures = check(con, path)
        con.close()
        return failures


def with_parent(taxon, parent):
    return [(r[0], parent, *r[2:]) if r[0] == taxon else r for r in GOOD]


class Invariants(unittest.TestCase):
    def assert_fails(self, rows, text):
        failures = run(rows)
        self.assertTrue(any(text in f for f in failures), failures)

    def test_good_tree_passes(self):
        self.assertEqual(run(GOOD), [])

    def test_two_parents(self):
        extra = (200, 300, "species", "Cepolaea a", "accepted", "Animalia")
        self.assert_fails(GOOD + [extra], "más de un camino")

    def test_duplicate_id(self):
        extra = (200, 300, "species", "Cepolaea a", "accepted", "Animalia")
        self.assert_fails(GOOD + [extra], "repetido")

    def test_orphan(self):
        self.assert_fails(with_parent(200, None), "sin padre")

    def test_dangling_parent(self):
        self.assert_fails(with_parent(200, 999), "padre inexistente")

    def test_cycle(self):
        rows = [(r[0], 200, *r[2:]) if r[0] == 100 else r for r in GOOD]
        self.assert_fails(rows, "inalcanzable")

    def test_child_above_parent(self):
        self.assert_fails(with_parent(100, 200), "rango superior")

    def test_wrong_kingdom(self):
        plantae = (2, -2, "kingdom", "Plantae", "accepted", "Plantae")
        rows = [(r[0], 2, *r[2:]) if r[0] == 300 else r for r in GOOD + [plantae]]
        self.assert_fails(rows, "distinto al de GBIF")

    def test_cepolidae_merged(self):
        rows = [r for r in GOOD if r[0] != 6755596]
        rows = [(r[0], 4257, *r[2:]) if r[0] == 300 else r for r in rows]
        self.assert_fails(rows, "Cepolidae")

    def test_cepolidae_wrong_lineage(self):
        self.assert_fails(with_parent(6755596, 10), "Cepolidae")


if __name__ == "__main__":
    unittest.main()
