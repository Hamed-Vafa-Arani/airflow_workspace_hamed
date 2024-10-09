from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pprint import pprint

# def print_context(**context):
#     pprint(context)

def _print_exec_date(**context):
    print("This script was executed at" + context["templates_dict"]["execution_date"])

with DAG(
    dag_id="3_context_python",
    start_date=datetime(year=2024, month=10, day=7),
    schedule="@daily",
):
    
    print_exec_date = PythonOperator(
        task_id="demo_templating",
        python_callable=_print_exec_date,
        provide_context=True,
        templates_dict={
        "execution_date": "{{ execution_date }}"
        },
        )

