from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.datasets import Dataset
from airflow.models import Variable
from datetime import datetime


VENV_PYTHON = "/home/vivek/Projects/data-assist/venv/bin/python"
PROJECT_DIR = "/home/vivek/Projects/data-assist"

PG_USER = Variable.get("pg_user", default_var="postgres")
PG_PASS = Variable.get("pg_password", default_var="postgres")

RETAIL_RAW_DATA = Dataset("postgres://localhost:5432/retail/retail_raw_batch")

with DAG(
    dag_id="retail_data_generator",
    start_date=datetime(2026, 1, 1),
    schedule="0 */6 * * *",     # every 6 hours
    catchup=False,
    tags=["retail", "generator"]
) as dag:

    generate = BashOperator(
        task_id="generate_data",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} pipelines/generator.py",
        env={
            "PG_USER": PG_USER,
            "PG_PASSWORD": PG_PASS
        },
        outlets=[RETAIL_RAW_DATA]
    )