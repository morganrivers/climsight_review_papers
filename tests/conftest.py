"""
Test configuration and fixtures for climsight review papers tests.
"""
import pytest
import tempfile
import os
from pathlib import Path


@pytest.fixture
def temp_api_keys_file():
    """Create a temporary API_KEYS.txt file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        f.write("# Test API Keys\n")
        f.write("ELSEVIER_API_KEY=test_api_key_12345\n")
        f.write("EMAIL_ADDRESS=test@example.com\n")
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    os.unlink(temp_path)


@pytest.fixture
def sample_work_data():
    """Sample OpenAlex work data for testing"""
    return {
        "id": "https://openalex.org/W2741809807",
        "display_name": "Climate change impacts on global agriculture",
        "doi": "https://doi.org/10.1038/nature12373",
        "publication_year": 2021,
        "cited_by_count": 156,
        "host_venue": {
            "display_name": "Nature Climate Change"
        },
        "best_oa_location": {
            "is_oa": True,
            "oa_status": "gold",
            "url": "https://example.com/paper.pdf",
            "url_for_pdf": "https://example.com/paper.pdf"
        },
        "primary_location": {
            "is_oa": True,
            "oa_status": "gold"
        },
        "primary_topic": {
            "id": "https://openalex.org/T10004"
        },
        "topics": [
            {"id": "https://openalex.org/T10889"},
            {"id": "https://openalex.org/T11275"}
        ],
        "sustainable_development_goals": [
            {"id": "https://metadata.un.org/sdg/2", "score": 0.89},
            {"id": "https://metadata.un.org/sdg/13", "score": 0.76},
            {"id": "https://metadata.un.org/sdg/15", "score": 0.23}
        ],
        "authorships": [
            {
                "institutions": [
                    {"country_code": "US"},
                    {"country_code": "UK"}
                ]
            },
            {
                "institutions": [
                    {"country_code": "CA"}
                ]
            }
        ],
        "language": "en",
        "citation_normalized_percentile": {
            "value": 0.95
        },
        "abstract_inverted_index": {
            "Climate": [0],
            "change": [1],
            "poses": [2],
            "significant": [3],
            "challenges": [4],
            "to": [5],
            "agriculture": [6]
        }
    }