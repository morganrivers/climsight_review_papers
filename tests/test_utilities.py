"""
Tests for utility functions in the climsight review papers codebase.
Focus on data processing and string manipulation functions.
"""
import unittest
import sys
import os
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

# Import functions to test
from download_openalex_matching import (
    decode_abstract, top_topic_ids, country_code_string, 
    sdg_pairs, extract_row, make_filter
)


class TestDataProcessingFunctions(unittest.TestCase):
    
    def test_decode_abstract_valid_input(self):
        """Test 1: decode_abstract reconstructs text correctly from inverted index"""
        inv_idx = {
            "Climate": [0, 5],
            "change": [1, 6], 
            "affects": [2],
            "global": [3],
            "weather": [4]
        }
        result = decode_abstract(inv_idx)
        expected = "Climate change affects global weather Climate change"
        self.assertEqual(result, expected)
    
    def test_decode_abstract_empty_input(self):
        """Test 2: decode_abstract handles None and empty inputs"""
        self.assertIsNone(decode_abstract(None))
        self.assertIsNone(decode_abstract({}))
    
    def test_top_topic_ids_extracts_correctly(self):
        """Test 3: top_topic_ids extracts and formats topic IDs properly"""
        work = {
            "primary_topic": {"id": "https://openalex.org/T10001"},
            "topics": [
                {"id": "https://openalex.org/T10002"},
                {"id": "https://openalex.org/T10003"},
                {"id": "https://openalex.org/T10004"}
            ]
        }
        result = top_topic_ids(work, k=3)
        expected = ["T10001", "T10002", "T10003"]
        self.assertEqual(result, expected)
    
    def test_top_topic_ids_handles_missing_data(self):
        """Test 4: top_topic_ids handles missing topics gracefully"""
        work = {"primary_topic": {"id": "https://openalex.org/T10001"}}
        result = top_topic_ids(work, k=3)
        expected = ["T10001", None, None]
        self.assertEqual(result, expected)
    
    def test_country_code_string_formats_correctly(self):
        """Test 5: country_code_string extracts and formats country codes"""
        work = {
            "authorships": [
                {"institutions": [{"country_code": "US"}, {"country_code": "CA"}]},
                {"institutions": [{"country_code": "US"}, {"country_code": "UK"}]}
            ]
        }
        result = country_code_string(work)
        expected = "CA;UK;US"  # sorted and deduplicated
        self.assertEqual(result, expected)
    
    def test_country_code_string_handles_empty_data(self):
        """Test 6: country_code_string handles missing authorship data"""
        work = {"authorships": []}
        result = country_code_string(work)
        self.assertIsNone(result)
    
    def test_sdg_pairs_filters_by_threshold(self):
        """Test 7: sdg_pairs correctly filters SDGs by score threshold"""
        work = {
            "sustainable_development_goals": [
                {"id": "https://metadata.un.org/sdg/3", "score": 0.95},
                {"id": "https://metadata.un.org/sdg/13", "score": 0.42},
                {"id": "https://metadata.un.org/sdg/7", "score": 0.15}
            ]
        }
        result = sdg_pairs(work, thresh=0.4)
        expected = "3|0.95;13|0.42"
        self.assertEqual(result, expected)
    
    def test_sdg_pairs_handles_no_sdgs(self):
        """Test 8: sdg_pairs handles works without SDG data"""
        work = {}
        result = sdg_pairs(work)
        self.assertIsNone(result)
    
    def test_make_filter_constructs_correct_string(self):
        """Test 9: make_filter constructs proper OpenAlex API filter string"""
        result = make_filter(10004, primary=True)
        expected_parts = [
            "primary_topic.id:T10004",
            "publication_year:2010-2025",
            "has_abstract:true",
            "has_doi:true"
        ]
        expected = ",".join(expected_parts)
        self.assertEqual(result, expected)
    
    def test_extract_row_transforms_work_data(self):
        """Test 10: extract_row correctly transforms OpenAlex work into CSV format"""
        work = {
            "id": "https://openalex.org/W12345",
            "display_name": "Climate Change Review",
            "doi": "https://doi.org/10.1234/example",
            "publication_year": 2023,
            "cited_by_count": 42,
            "host_venue": {"display_name": "Nature Climate Change"},
            "best_oa_location": {"is_oa": True, "oa_status": "gold"},
            "primary_topic": {"id": "https://openalex.org/T10001"},
            "topics": [],
            "language": "en",
            "abstract_inverted_index": {"Climate": [0], "change": [1]}
        }
        
        result = extract_row(work)
        
        self.assertEqual(result["openalex_id"], "https://openalex.org/W12345")
        self.assertEqual(result["title"], "Climate Change Review")
        self.assertEqual(result["doi"], "https://doi.org/10.1234/example")
        self.assertEqual(result["publication_year"], 2023)
        self.assertEqual(result["cited_by_count"], 42)
        self.assertEqual(result["journal"], "Nature Climate Change")
        self.assertTrue(result["is_oa"])
        self.assertEqual(result["oa_status"], "gold")
        self.assertEqual(result["topic_id_1"], "T10001")
        self.assertEqual(result["language"], "en")
        self.assertEqual(result["abstract"], "Climate change")


if __name__ == '__main__':
    unittest.main()