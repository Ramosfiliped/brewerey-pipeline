"""
Pytest configuration file for the brewery pipeline tests
"""
import os
import sys
import pytest

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Add the dags folder to Python path
dags_path = os.path.join(project_root, "dags")
sys.path.insert(0, dags_path)


@pytest.fixture(scope="session")
def test_data_dir():
    """Fixture to provide test data directory"""
    return os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture(scope="session")
def mock_brewery_data():
    """Fixture to provide mock brewery data for testing"""
    return [
        {
            "id": "1",
            "name": "Test Brewery 1",
            "brewery_type": "micro",
            "city": "Test City 1",
            "state": "Test State 1",
            "country": "United States",
            "longitude": "-122.4324",
            "latitude": "37.7749"
        },
        {
            "id": "2",
            "name": "Test Brewery 2", 
            "brewery_type": "regional",
            "city": "Test City 2",
            "state": "Test State 2",
            "country": "United States",
            "longitude": "-74.0060",
            "latitude": "40.7128"
        },
        {
            "id": "3",
            "name": "Test Brewery 3",
            "brewery_type": "large",
            "city": "Test City 3", 
            "state": "Test State 3",
            "country": "United States",
            "longitude": "-87.6298",
            "latitude": "41.8781"
        }
    ] 