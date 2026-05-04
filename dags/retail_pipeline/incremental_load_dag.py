from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.datasets import Dataset

RETAIL_RAW_DATA = Dataset("postgres://localhost:5432/retail/retail_raw_batch")

VENV_PYTHON = "/home/vivek/Projects/data-assist/venv/bin/python"
PROJECT_DIR = "/home/vivek/Projects/data-assist"

with DAG(
    dag_id="retail_incremental_load",
    start_date=datetime(2026, 1, 1),
    schedule = [RETAIL_RAW_DATA],
    # schedule="30 */6 * * *",    # every 6 hours at 30 min mark
    catchup=False,
    tags=["retail", "incremental"]
) as dag:

    load = BashOperator(
        task_id="incremental_load",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} pipelines/loader.py"
    )