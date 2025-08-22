from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    task1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from Airflow!'"
    )
