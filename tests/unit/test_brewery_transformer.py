import pytest
import json
import pandas as pd

from pandas import DataFrame
from unittest.mock import patch
from modules.storage.local_data_lake import LocalDataLake
from dags.brewery.service.brewery_transform import BreweryTransform

class TestBreweryTransform:
    """Test cases for BreweryTransform class"""

    def setup_method(self):
        self.transformer = BreweryTransform()

    @patch('modules.storage.local_data_lake.LocalDataLake')
    def test_transform_brewery_data_success(self, mock_data_lake):
        test_data = [
            {
                "id": "1",
                "name": "Brewery A",
                "brewery_type": "micro",
                "country": "United States",
                "city": "New York"
            },
            {
                "id": "2",
                "name": "Brewery B",
                "brewery_type": "regional",
                "country": "United States",
                "city": "Chicago"
            },
            {
                "id": "3",
                "name": "Brewery C",
                "brewery_type": "brewpub",
                "country": "Canada",
                "city": "Toronto"
            }
        ]
        
        mock_instance = mock_data_lake.return_value
        mock_instance.retrieve_data.return_value = json.dumps(test_data).encode('utf-8')

        result = self.transformer.transform_brewery_data("tests/dlake/bronze/file.json")

        assert isinstance(result, dict)
        assert len(result) == 2
        assert "United States" in result
        assert "Canada" in result
        
        us_df = result["United States"]
        assert isinstance(us_df, pd.DataFrame)
        assert len(us_df) == 2
        assert set(us_df['name']) == {"Brewery A", "Brewery B"}
        
        ca_df = result["Canada"]
        assert isinstance(ca_df, pd.DataFrame)
        assert len(ca_df) == 1
        assert ca_df.iloc[0]['name'] == "Brewery C"

    @patch('modules.storage.local_data_lake.LocalDataLake')
    def test_transform_brewery_data_empty(self, mock_data_lake):
        """Test transformation with empty data"""
        # Mock empty data
        mock_instance = mock_data_lake.return_value
        mock_instance.retrieve_data.return_value = json.dumps([]).encode('utf-8')

        # Call the method
        result = self.transformer.transform_brewery_data("tests/dlake/bronze/empty.json")

        # Assertions
        assert isinstance(result, DataFrame)
        assert len(result) == 0  # Should be empty
