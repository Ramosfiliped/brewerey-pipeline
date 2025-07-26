from datetime import datetime
from airflow.decorators import dag, task
from brewery.service.brewery_extractor import BreweryExtractor

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

        while True:
            breweries = extractor.extract_brewery_info(items_per_page=200, page=page)
            page += 1
            if len(breweries) == 0:
                break
            
            print(f"Extracted {len(breweries)} breweries from page {page}")
            # Save data on dlake

    @task
    def transform():
        print("Transforming")

    @task
    def load():
        print("Loading")

    @task
    def finish():
        print("Logging the end of the DAG")

    start() >> extract() >> transform() >> load() >> finish()

brewery_dag()
