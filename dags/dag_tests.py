from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='dag_tests',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='DAG to run dbt tests for all layers',
    tags=["tests"],
) as dag:

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dbt && dbt test',
    )

    run_dbt_tests