import unittest
from foia_bias.utils.corpus_config import validate_corpus_config

def valid():
    return {"corpus":{"mode":"snapshot","manifest_path":"fixtures/manifest.json","fixture_mode":True,"batch_size":100},"cohort":{"start_date":"1993-01-20","end_date":"2025-01-20","include_incomplete_administrations":False}}

class CorpusConfigTests(unittest.TestCase):
    def test_valid(self):
        self.assertEqual(validate_corpus_config(valid()), valid())
    def test_missing_manifest(self):
        x=valid();x["corpus"]["manifest_path"]=""
        with self.assertRaises(ValueError):validate_corpus_config(x)
    def test_path_escape(self):
        x=valid();x["corpus"]["manifest_path"]="../secret"
        with self.assertRaises(ValueError):validate_corpus_config(x)
    def test_wrong_mode(self):
        x=valid();x["corpus"]["mode"]="crawler"
        with self.assertRaises(ValueError):validate_corpus_config(x)
    def test_future_cohort(self):
        x=valid();x["cohort"]["end_date"]="2026-01-20"
        with self.assertRaises(ValueError):validate_corpus_config(x)
    def test_population_cap_not_batch(self):
        x=valid();x["corpus"]["batch_size"]=0
        with self.assertRaises(ValueError):validate_corpus_config(x)

if __name__=="__main__":
    unittest.main()
