from datetime import datetime
from airflow.decorators import dag, task


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
        print("Logging the start of the DAG")

    @task
    def extract():
        print("Extracting")

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
