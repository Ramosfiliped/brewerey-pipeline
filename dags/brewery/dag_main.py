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
        print("Starting the brewery DAG")

    @task
    def brew_beer():
        print("Brewing beer...")

    @task
    def finish():
        print("Finishing the brewing process")

    start() >> brew_beer() >> finish()

brewery_dag()