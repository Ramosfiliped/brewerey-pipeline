import io
import pandas as pd
from modules.storage.local_data_lake import LocalDataLake

class BreweryLoader:
    def load_brewery_data(self, file_path: str) -> None:
        """
        Create an aggregated view with the quantity of breweries per type and location.
        
        Args:
            file_path (str): Path to the brewery data file.
            
        Returns:
            DataFrame: Aggregated view with brewery counts by type and location.
        """
        file = LocalDataLake().retrieve_data(file_name=f"dlake/{file_path}")
        df = pd.read_parquet(io.BytesIO(file))
        
        aggregated_by_country = df.groupby(['brewery_type', 'country']).size().reset_index(name='count')
        print(aggregated_by_country.head())
        
        return aggregated_by_country
