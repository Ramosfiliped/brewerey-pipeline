import json
import pandas as pd

from datetime import datetime
from airflow.decorators import dag, task
from brewery.service.brewery_loader import BreweryLoader
from modules.storage.local_data_lake import LocalDataLake
from brewery.service.brewery_extractor import BreweryExtractor
from brewery.service.brewery_transform import BreweryTransform

@dag(
    start_date=datetime(2025, 7, 26),
    catchup=False,
    tags=['brewery'],
    default_args={
        'owner': 'airflow',
        'retries': 3,
        'retry_delay': 60
    },
    max_active_runs=1,
    max_active_tasks=3,
    dag_id='brewery_dag',
)
def brewery_dag():
    @task
    def start():
        print("Starting the brewery extraction DAG")

    @task
    def extract():
        """
            Extract brewery information from the brewery API and save
            on data lake on bronze layer
        """
        extractor = BreweryExtractor()
        page = 1
        file_paths = []

        while True:
            breweries = extractor.extract_brewery_info(items_per_page=200, page=page)
            if len(breweries) == 0:
                break
            
            print(f"Extracted {len(breweries)} breweries from page {page}")
            file_path = LocalDataLake().save_on_storage(
                dataset='BREWERY',
                layer='bronze',
                file_name=f'breweries_page_{page}',
                file_content=json.dumps(breweries, indent=4, ensure_ascii=False).encode('utf-8'),
                file_format='json'
            )

            file_paths.append(file_path)
            page += 1

        return file_paths

    @task
    def transform(raw_file_paths: list):
        transformer = BreweryTransform()
        transformed_files = []
        for index, file_path in enumerate(raw_file_paths):
            transformed_dfs = transformer.transform_brewery_data(file_path)
            for country, df in transformed_dfs.items():
                transformed_file_path = LocalDataLake().save_on_storage(
                    dataset=f'BREWERY/{country}',
                    layer='silver',
                    file_name=f'transformed_{index}',
                    file_content=df.to_parquet(),
                    file_format='parquet'
                )
                transformed_files.append(transformed_file_path)
        
        return transformed_files

    @task
    def load(transformed_files: list):
        loader = BreweryLoader()
        aggregated_files = []
        """
            Load the transformed brewery data and create an aggregated view
            with the quantity of breweries per type and location.
        """
        country_files = {}
        for file_path in transformed_files:
            country = file_path.split('/')[2]
            if country not in country_files:
                country_files[country] = []
            country_files[country].append(file_path)
        
        for country, file_paths in country_files.items():
            print(f"Processando país: {country}")
            
            country_dfs = []
            
            for file_path in file_paths:
                aggregated_view = loader.load_brewery_data(file_path)
                country_dfs.append(aggregated_view)
            
            if country_dfs:
                combined_df = pd.concat(country_dfs, ignore_index=True)
                
                final_aggregated = combined_df.groupby(['brewery_type', 'country']).agg({
                    'count': 'sum'
                }).reset_index()
                
                aggregated_file_path = LocalDataLake().save_on_storage(
                    dataset='BREWERY',
                    layer='gold',
                    file_name=f'aggregated_{country}',
                    file_content=final_aggregated.to_parquet(index=False),
                    file_format='parquet'
                )
                aggregated_files.append(aggregated_file_path)
        
        return aggregated_files

    @task
    def finish():
        print("Logging the end of the DAG")

    extracted_files = extract()
    transformed_files = transform(extracted_files)
    _load = load(transformed_files)
    _finish = finish()

    start() >> _load >> _finish

brewery_dag()
