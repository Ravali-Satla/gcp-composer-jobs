# import statements
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from functions.send_email import send_task_failure_email

schedule_interval="0 */4 * * *"
default_args = {
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG definitions
with DAG(dag_id='load-gcs-to-bq',
         description='A DAG to load data from GCS to BigQuery',
         catchup=False,
         schedule_interval=schedule_interval,
         start_date=datetime(2024,1,1),
         default_args=default_args
         ) as dag:

# Dummy strat task   
    start = DummyOperator(
        task_id='start',
        dag=dag,
       on_failure_callback=send_task_failure_email
    )

# GCS to BigQuery data load Operator and task
    gcs_to_bq_load = GoogleCloudStorageToBigQueryOperator(
                task_id='load-gcs-to-bq',
                bucket='project-dataengineering-demo',
                source_objects=['bat.csv'],
                destination_project_dataset_table='forward-curve-415300.cricket.gcs_to_bq_table',
                schema_fields=[
                                {'name': 'player', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'Team', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'Type', 'type': 'STRING', 'mode': 'NULLABLE'},
                                {'name': 'Runs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                              ],
                skip_leading_rows=1,
                create_disposition='CREATE_IF_NEEDED',
                write_disposition='WRITE_TRUNCATE', 
    dag=dag)



# Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Settting up task  dependency
start >> gcs_to_bq_load  >> end