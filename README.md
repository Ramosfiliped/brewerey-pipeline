# Brewery Pipeline

Data pipeline for brewery extract in ETL format using Airflow

## How to run

### Init Airflow
```bash
docker compose run airflow-cli airflow config list
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

## To Do
### Pyspark
In real case scenarios, DAGs can be trigger multiple times a day.
And have much more data than brewery dataset.
Use pyspark in this scenarios will has more speed to process all the data.
The solution implemented in this repository has the benefit of process the data
in chuncks, so needs less infrastructure, but is slower.

### K8s
Use Kubernetes as a infra has the benefit of guarantee that the docker will be always up.
And if something happen, the k8s can take care of it.

### Monitoring Module
Use some monitoring tool as grafana will give some visualize of how the process is going.
If some DAG is failing too much, and the time of the DAGs

### Notifications Module
Some type of failures needs to have some type of action by the developer.
So implement a logging module in the follwing type.

|-------------|-----------------------------------------------------------------------------------------------------|
| Severity    | Description                                                                                         |
|-------------|-----------------------------------------------------------------------------------------------------|
| DEBUG       | Info to debug                                                                                       |
| INFO        | Info of rotine and continuos status                                                                 |
| WARNING     | Info of something can cause a problem                                                               |
| ERROR       | Info of something that goes wrong                                                                   |
| CRITICAL    | Event of something that can cause interruptions on the system                                       |
| ALERT       | Someone has to take an action imediatly                                                             |
| EMERGENCY   | One or more services isn't working                                                                  |
|-------------|-----------------------------------------------------------------------------------------------------|

The **ERROR** type will be notified on a common local like slack, discord, or teams channel
**CRITICAL** events needs to notify more directly the leader of the team
**ALERT** and **EMERGENCY** notify by SMS or phone call directly to take an instant action