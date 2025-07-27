# Brewery Pipeline

Data pipeline for brewery extract in ETL format using Airflow

## How to run

### Init Airflow
```bash
docker compose up airflow-init
docker compose up
```

### Run DAG
On your browser, with the airflow running on your terminal access the [webserver](http://localhost:8080/).
The login and password is **airflow**.
On the left side has and menu.
Click on Dags, then brewery_dag and trigger.
The files will be created on the dlake foulder on your PC.

## Run Tests
```bash
docker build -t brewery-tests -f tests/Dockerfile .
docker run -t --rm brewery-tests
```
