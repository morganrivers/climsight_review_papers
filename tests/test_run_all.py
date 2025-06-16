#!/usr/bin/env python3
"""
Simple test runner that runs all tests in the tests/ directory.
Run with: python tests/test_run_all.py
"""
import unittest
import sys
import os

# Add the tests directory to Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import test modules
from test_utilities import TestDataProcessingFunctions
from test_string_processing import TestStringProcessingFunctions


def run_all_tests():
    """Run all test suites"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test suites
    suite.addTests(loader.loadTestsFromTestCase(TestDataProcessingFunctions))
    suite.addTests(loader.loadTestsFromTestCase(TestStringProcessingFunctions))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Return success/failure
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)