import requests

class BreweryExtractor:
    def extract_brewery_info(self, items_per_page: int = 3, page: int = 1) -> dict:
        """
        Extract brewery information from the brewery API
        Args:
            items_per_page (int): Number of items to fetch per page
            page (int): Page number to fetch
        """

        response = requests.get(
            url=f"https://api.openbrewerydb.org/v1/breweries?"
                f"per_page={items_per_page}"
                f"&page={page}"
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code}")

        return response.json()
