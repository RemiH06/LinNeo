"""match_taxon debe acertar lo seguro y rechazar lo ambiguo.

    python -m unittest discover -s pipeline/tests
"""
import sys
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from match import match_taxon  # noqa: E402

BACKBONE = [
    (1, "Felis", "Animalia"),
    (2, "Felis leo", "Animalia"),      # sinónimo de 5
    (3, "Cepola", "Animalia"),
    (4, "Cepola", "Plantae"),
    (5, "Panthera leo", "Animalia"),
    (6, "Vaga", "Plantae"),            # doubtful, no es nodo
    (7, "Vaga", "Animalia"),
    (8, "Solo sinonimo", "Animalia"),  # sinónimo de 5
    (9, "Solo sinonimo", "Plantae"),   # sinónimo de 4
    (10, "Otro sinonimo", "Plantae"),  # sinónimo de 4
]
NODE_OF = [(1, 1), (3, 3), (4, 4), (5, 5), (7, 7), (2, 5), (8, 5), (9, 4), (10, 4)]
NAMES = [
    ("felis", 1, 1, "Animalia"),
    ("cepola", 3, 1, "Animalia"),
    ("cepola", 4, 1, "Plantae"),
    ("panthera leo", 5, 1, "Animalia"),
    ("vaga", 7, 1, "Animalia"),
    ("felis leo", 5, 2, "Animalia"),
    ("solo sinonimo", 5, 2, "Animalia"),
    ("solo sinonimo", 4, 2, "Plantae"),
    ("otro sinonimo", 4, 2, "Plantae"),
]


def run(rows):
    con = duckdb.connect()
    for table, spec, data in (
        ("backbone", "id BIGINT, name VARCHAR, kingdom VARCHAR", BACKBONE),
        ("node_of", "id BIGINT, taxonID BIGINT", NODE_OF),
        ("name_index", "name VARCHAR, taxonID BIGINT, tier INT, kingdom VARCHAR", NAMES),
    ):
        con.execute(f"CREATE TEMP TABLE {table}({spec})")
        con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * len(data[0]))})", data)
    con.execute("CREATE TEMP TABLE q(qkey VARCHAR, gbif BIGINT, name VARCHAR, kingdom VARCHAR)")
    con.executemany("INSERT INTO q VALUES (?, ?, ?, ?)", rows)
    match_taxon(con, "q")
    matched = {k: (t, m) for k, t, m in con.execute("SELECT * FROM matched").fetchall()}
    unmatched = dict(con.execute("SELECT qkey, reason FROM unmatched").fetchall())
    con.close()
    return matched, unmatched


class MatchTaxon(unittest.TestCase):
    def outcome(self, *row):
        matched, unmatched = run([("a", *row)])
        return matched.get("a") or unmatched["a"]

    def test_id_to_node(self):
        self.assertEqual(self.outcome(5, None, None), (5, "id"))

    def test_id_of_synonym_goes_to_accepted(self):
        self.assertEqual(self.outcome(2, None, None), (5, "id"))

    def test_id_beats_a_contradicting_name(self):
        self.assertEqual(self.outcome(4, "Cepola", "Animalia"), (4, "id"))

    def test_two_ids_for_one_record_are_ambiguous(self):
        matched, unmatched = run([("a", 3, None, None), ("a", 5, None, None)])
        self.assertEqual((matched, unmatched), ({}, {"a": "ambiguous_id"}))

    def test_two_ids_of_the_same_taxon_are_fine(self):
        matched, _ = run([("a", 5, None, None), ("a", 2, None, None)])
        self.assertEqual(matched, {"a": (5, "id")})

    def test_unique_name(self):
        self.assertEqual(self.outcome(None, "FELIS", None), (1, "name"))

    def test_homonym_without_context_is_rejected(self):
        self.assertEqual(self.outcome(None, "Cepola", None), "ambiguous_name")

    def test_homonym_with_kingdom(self):
        self.assertEqual(self.outcome(None, "Cepola", "Plantae"), (4, "name"))

    def test_synonym_name(self):
        self.assertEqual(self.outcome(None, "Felis leo", None), (5, "synonym_name"))

    def test_ambiguous_synonym_is_rejected(self):
        self.assertEqual(self.outcome(None, "Solo sinonimo", None), "ambiguous_name")

    def test_not_found(self):
        self.assertEqual(self.outcome(None, "Nadie", None), "not_found")

    def test_non_node_id_does_not_fall_into_other_kingdom(self):
        self.assertEqual(self.outcome(6, "Vaga", None), "not_found")

    def test_no_name_and_no_node(self):
        self.assertEqual(self.outcome(6, None, None), "not_found")


if __name__ == "__main__":
    unittest.main()
