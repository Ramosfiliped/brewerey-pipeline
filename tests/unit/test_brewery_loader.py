import pytest
import pandas as pd
from unittest.mock import patch
from dags.brewery.service.brewery_loader import BreweryLoader

class TestBreweryLoader:
    """Test cases for BreweryLoader class"""

    def setup_method(self):
        self.loader = BreweryLoader()

    @patch('modules.storage.local_data_lake.LocalDataLake')
    @patch('pandas.read_parquet')
    def test_load_brewery_data_success(self, mock_read_parquet, mock_data_lake):
        test_data = pd.DataFrame({
            'brewery_type': ['micro', 'micro', 'regional', 'nano', 'micro'],
            'country': ['US', 'US', 'UK', 'UK', 'CA'],
            'name': ['Brewery A', 'Brewery B', 'Brewery C', 'Brewery D', 'Brewery E']
        })

        expected_result = pd.DataFrame({
            'brewery_type': ['micro', 'micro', 'nano', 'regional'],
            'country': ['US', 'CA', 'UK', 'UK'],
            'count': [2, 1, 1, 1]
        })

        mock_read_parquet.return_value = test_data
        
        mock_instance = mock_data_lake.return_value
        mock_instance.retrieve_data.return_value = b'mock_parquet_data'

        result = self.loader.load_brewery_data("tests/dlake/silver/breweries.parquet")

        pd.testing.assert_frame_equal(
            result.sort_values(by=['brewery_type', 'country']).reset_index(drop=True),
            expected_result.sort_values(by=['brewery_type', 'country']).reset_index(drop=True)
        )

        mock_read_parquet.assert_called_once()

    @patch('modules.storage.local_data_lake.LocalDataLake')
    @patch('pandas.read_parquet')
    def test_load_brewery_data_empty(self, mock_read_parquet, mock_data_lake):
        """Test aggregation with empty dataset"""
        mock_read_parquet.return_value = pd.DataFrame(columns=['brewery_type', 'country', 'name'])
        
        mock_instance = mock_data_lake.return_value
        mock_instance.retrieve_data.return_value = b'mock_parquet_data'

        # Call the method
        result = self.loader.load_brewery_data("tests/dlake/silver/empty.parquet")

        # Assertions
        assert result.empty
        assert list(result.columns) == ['brewery_type', 'country', 'count']
