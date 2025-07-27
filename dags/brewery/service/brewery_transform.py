import json
import pandas as pd

from pandas import DataFrame
from modules.storage.local_data_lake import LocalDataLake

class BreweryTransform:
    """    
    Class to handle transformations related to breweries.
    """
    def transform_brewery_data(self, file_path: str) -> DataFrame:
        """
        Transform brewery data from the bronze layer to the silver layer.
        
        Returns:
            DataFrame: Transformed brewery data.
        """
        
        file = LocalDataLake().retrieve_data(file_name=f"dlake/{file_path}")
        raw_data = json.loads(file.decode('utf-8'))
        df = pd.DataFrame(raw_data)
        unique_countries = df['country'].unique()

        country_dfs = {}

        for country in unique_countries:
            country_dfs[country] = df[df['country'] == country]
        
        return country_dfs
