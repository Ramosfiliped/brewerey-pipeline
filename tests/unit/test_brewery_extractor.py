import pytest
import requests
from unittest.mock import Mock, patch
from dags.brewery.service.brewery_extractor import BreweryExtractor


class TestBreweryExtractor:
    """Test cases for BreweryExtractor class"""

    def setup_method(self):
        """Setup method to initialize the extractor before each test"""
        self.extractor = BreweryExtractor()

    def test_extractor_initialization(self):
        """Test that BreweryExtractor can be instantiated"""
        extractor = BreweryExtractor()
        assert extractor is not None
        assert isinstance(extractor, BreweryExtractor)

    @patch('requests.get')
    def test_extract_brewery_info_success(self, mock_get):
        """Test successful extraction of brewery information"""
        mock_response_data = [
            {
                "id": "5128df48-79fc-4f0f-8b52-d06be54d0cec",
                "name": "(405) Brewing Co",
                "brewery_type": "micro",
                "address_1": "1716 Topeka St",
                "address_2": None,
                "address_3": None,
                "city": "Norman",
                "state_province": "Oklahoma",
                "postal_code": "73069-8224",
                "country": "United States",
                "longitude": -97.46818222,
                "latitude": 35.25738891,
                "phone": "4058160490",
                "website_url": "http://www.405brewing.com",
                "state": "Oklahoma",
                "street": "1716 Topeka St"
            },
            {
                "id": "9c5a66c8-cc13-416f-a5d9-0a769c87d318",
                "name": "(512) Brewing Co",
                "brewery_type": "micro",
                "address_1": "407 Radam Ln Ste F200",
                "address_2": None,
                "address_3": None,
                "city": "Austin",
                "state_province": "Texas",
                "postal_code": "78745-1197",
                "country": "United States",
                "longitude": None,
                "latitude": None,
                "phone": "5129211545",
                "website_url": "http://www.512brewing.com",
                "state": "Texas",
                "street": "407 Radam Ln Ste F200"
            }
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_response_data.copy()
        mock_get.return_value = mock_response

        result = self.extractor.extract_brewery_info(items_per_page=2, page=1)

        assert sorted(result, key=lambda x: x['id']) == sorted(mock_response_data, key=lambda x: x['id'])

        mock_get.assert_called_once_with(
            url="https://api.openbrewerydb.org/v1/breweries?per_page=2&page=1"
        )
