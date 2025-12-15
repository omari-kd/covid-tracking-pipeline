from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 12, 15),
    "retries": 1,
}

with DAG(
    dag_id="covid_etl_pipeline-3",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="ingest_covid_data",
        # bash_command="python /app/src/etl/ingest_covid.py"
        bash_command="python -m src.etl.ingest_covid",
        cwd="/home/kojo/Projects/Data-Engineering-Projects/covid-tracking-pipeline"
    )

    transform = BashOperator(
        task_id="transform_covid_data",
        # bash_command="python /app/src/etl/transform_covid.py"
        bash_command="python -m src.etl.transform_covid",
        cwd="/home/kojo/Projects/Data-Engineering-Projects/covid-tracking-pipeline"

    )

    ingest >> transform
