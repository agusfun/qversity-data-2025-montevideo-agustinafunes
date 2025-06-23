 # Master pipeline DAG to run all stages in order

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 6, 1),
}

with DAG(
    dag_id='dag_pipeline',
    schedule_interval=None,  # Manual
    catchup=False,
    default_args=default_args,
    description='Master DAG that triggers Bronze, Silver and Gold DAGs in sequence',
    tags=["pipeline"]
) as dag:

    trigger_bronze = TriggerDagRunOperator(
        task_id='trigger_bronze',
        trigger_dag_id='dag_bronze',
        wait_for_completion=True
    )

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver',
        trigger_dag_id='dag_silver',
        wait_for_completion=True
    )

    trigger_gold = TriggerDagRunOperator(
         task_id='trigger_gold',
         trigger_dag_id='dag_gold',
         wait_for_completion=True
    )

    trigger_bronze >> trigger_silver >> trigger_gold