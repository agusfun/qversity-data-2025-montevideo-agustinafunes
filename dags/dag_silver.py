# DAG to run DBT models for the Silver layer

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='dag_silver',
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    description='Runs DBT models for Silver layer',
    tags=["silver"]
) as dag:

    run_dbt_silver = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt run --select silver'
    )

    run_dbt_silver