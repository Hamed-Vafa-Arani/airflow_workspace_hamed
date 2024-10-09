from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pprint import pprint

def print_context(**context):
    pprint(context)

with DAG(
    dag_id="3_context",
    start_date=datetime(year=2024, month=10, day=7),
    schedule="@daily",
):
    # task_1 = PythonOperator(
    #     task_id = "context_print",
    #     python_callable = print_context,
    # )

    task_2 = BashOperator(
        task_id = "Bash_reading",
        bash_command = "echo {{task.task_id}} is running in the {{dag.dag_id}} pipeline"
    )

