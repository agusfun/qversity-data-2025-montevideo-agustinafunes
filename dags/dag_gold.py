# DAG that triggers dbt models for the Gold layer

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='dag_gold',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["gold"],
) as dag:

    run_dbt_gold = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && dbt run --select gold',
    )

    run_dbt_gold