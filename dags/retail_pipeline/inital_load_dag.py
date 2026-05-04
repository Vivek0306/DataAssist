from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime

VENV_PYTHON = "/home/vivek/Projects/data-assist/venv/bin/python"
PROJECT_DIR = "/home/vivek/Projects/data-assist"

PG_USER = Variable.get("pg_user", default_var="postgres")
PG_PASS = Variable.get("pg_password", default_var="postgres")

with DAG(
    dag_id="retail_initial_load",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # manual trigger only
    catchup=False,
    tags=["retail", "initial"]
) as dag:
    
    create_tables = BashOperator(
        task_id="create_tables",
        bash_command=f"PGPASSWORD=$PG_PASSWORD psql -h localhost -U $PG_USER -d retail -w -f {PROJECT_DIR}/sql/create_tables.sql",   
        env={
            "PG_USER": PG_USER,
            "PG_PASSWORD": PG_PASS
        }
    )

    extract = BashOperator(
        task_id="extract_to_raw",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} pipelines/extractor.py"
    )

    load = BashOperator(
        task_id="load_to_staging",
        bash_command=f"cd {PROJECT_DIR} && {VENV_PYTHON} pipelines/loader.py"
    )

    create_tables >> extract >> load