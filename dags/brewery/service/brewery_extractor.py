import requests

class BreweryExtractor:
    def extract_brewery_info(self, items_per_page=3, page=1):
        """
        Extract brewery information from the brewery API and save
        on data lake on bronze layer
        """

        response = requests.get(
            url=f"https://api.openbrewerydb.org/v1/breweries?"
                f"per_page={items_per_page}"
                f"&page={page}"
        )

        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code}")

        return response.json()
