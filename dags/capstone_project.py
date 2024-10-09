from datetime import datetime, timedelta
import json
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import HttpOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.bigquery_to_postgres import BigQueryToPostgresOperator

def _check_if_any_launches(task_instance, **_):
    result_by_key = json.loads(task_instance.xcom_pull(task_ids="get_launch_data"))
    no_launch = result_by_key["count"] == 0
    if no_launch:
        raise AirflowSkipException('No launches today, skipping the rest of the process.')


def _preprocessing(task_instance, **context):
    response = json.loads(task_instance.xcom_pull(task_ids="get_launch_data"))
    data = pd.DataFrame([
        {
            "id": launch["id"],
            "name": launch["name"],
            "mission_name": launch["mission"]["name"],
            "launch_status": launch["status"]["name"],
            "country_name": launch["pad"]["country"]["name"],
            "launch_service_provider": launch["launch_service_provider"]["name"],
            "launch_service_provider_type": launch["launch_service_provider"]["type"]["name"]
        } 
        for launch in response["results"]
    ])
    data.to_parquet(f"/tmp/{context['ds']}.parquet")

with DAG(
    dag_id="capstone_project",
    start_date=datetime(year=2024, month=9, day=15),
    schedule=timedelta(days=1),
):
    check_api = HttpSensor(
        task_id='check_api',
        http_conn_id='thespacedevs_dev', 
        endpoint='',
        method='GET',
        timeout=10,  
        poke_interval=10  
    )

    get_launch_data = HttpOperator(
        task_id='get_launch_data',
        http_conn_id='thespacedevs_dev',  
        endpoint="launches",
        method='GET', 
        data={"window_start__gte": "{{ data_interval_start }}", "window_end__lt": " {{ data_interval_end }}"},
        # response_filter=lambda response: response.text,
        log_response = True
    )

    check_for_launches = PythonOperator(
        task_id = "check_if_any_launches",
        python_callable=_check_if_any_launches,
        provide_context=True,

        )

    prepprocessing = PythonOperator(
        task_id = "preprocessing",
        python_callable=_preprocessing,

    )

    moving_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "moving_to_gcs",
        src='/tmp/{{ds}}.parquet', 
        dst='Hamed/{{ds}}.parquet', 
        bucket='airflow-bol-2024-10-09',  
        gcp_conn_id='google_cloud_connection',  
    )

    store_in_bigquery = GCSToBigQueryOperator(
        task_id='store_in_bigquery',
        bucket='airflow-bol-2024-10-09',  
        source_objects=['Hamed/{{ds}}.parquet'],  
        destination_project_dataset_table='airflow-bol-2024-10-09.Hamed.launches', 
        source_format='PARQUET', 
        skip_leading_rows=1,  
        gcp_conn_id='google_cloud_connection',  
        write_disposition='WRITE_TRUNCATE'
    )

    postgres_db = PostgresOperator(
        task_id='postgres_db',
        postgres_conn_id='postgres',  
        sql='''
        CREATE TABLE IF NOT EXISTS launches (
            id VARCHAR(255),
            name VARCHAR(255),
            mission_name VARCHAR(255),
            launch_status VARCHAR(255),
            country_name VARCHAR(255),
            launch_service_provider VARCHAR(255),
            launch_service_provider_type VARCHAR(255)
            )
        '''
    )

    bigquery_to_postgres = BigQueryToPostgresOperator(
    task_id="bigquery_to_postgres",
    postgres_conn_id='postgres',
    gcp_conn_id='google_cloud_connection',  
    dataset_table=f"Hamed.launches",
    target_table_name='launches'
)


    check_api >> get_launch_data >> check_for_launches >> prepprocessing >> moving_to_gcs >> store_in_bigquery >> postgres_db >> bigquery_to_postgres