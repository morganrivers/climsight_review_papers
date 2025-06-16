# test_string_processing.py

import unittest
import sys
import os

# Adjust path to import from src
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from D_download_fulltexts import (
    sanitize_doi, safe_filename, looks_like_pdf
)

class TestStringProcessingFunctions(unittest.TestCase):
    
    def test_sanitize_doi_removes_url_prefix(self):
        result = sanitize_doi("https://doi.org/10.1234/example")
        expected = "10.1234/example"
        self.assertEqual(result, expected)

    def test_safe_filename_sanitizes_text(self):
        result = safe_filename("Climate Change: A Review (2023)")
        expected = "Climate_Change_A_Review_2023_"
        self.assertEqual(result, expected)

        long_text = "A" * 100
        result = safe_filename(long_text, limit=50)
        self.assertEqual(len(result), 50)

    def test_looks_like_pdf_identifies_pdf_content(self):
        pdf_content = b"%PDF-1.4" + b"x" * 10_000
        self.assertTrue(looks_like_pdf(pdf_content))

        small_pdf = b"%PDF-1.4" + b"x" * 100
        self.assertFalse(looks_like_pdf(small_pdf))

        not_pdf = b"<html>" + b"x" * 10_000
        self.assertFalse(looks_like_pdf(not_pdf))


if __name__ == '__main__':
    unittest.main()
