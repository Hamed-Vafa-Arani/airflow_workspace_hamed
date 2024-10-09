from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="1_first_dag",
    start_date=datetime(year=2024, month=10, day=8),
    end_date=datetime(year=2024, month=10, day=8),
    schedule="@daily",
):
    procure_rocket_material = EmptyOperator(task_id="procure_rocket_material")
    procure_fuel = EmptyOperator(task_id="procure_fuel")
    build_stage_1 = EmptyOperator(task_id="build_stage_1")
    build_stage_2 = EmptyOperator(task_id="build_stage_2")
    build_stage_3 = EmptyOperator(task_id="build_stage_3")
    launch = EmptyOperator(task_id="launch")

    [procure_rocket_material, procure_fuel] >> build_stage_3
    procure_rocket_material >> [build_stage_1, build_stage_2]
    [build_stage_1, build_stage_2, build_stage_3] >> launch
