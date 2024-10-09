from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

start_date = datetime(year=2024, month=10, day=8) - timedelta(days=3)

with DAG(
    dag_id="2_second_dag_b",
    start_date=start_date,
    schedule_interval=timedelta(days=3),  # Run every 3 days
    catchup=False,  
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
