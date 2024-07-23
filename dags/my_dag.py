

#############
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataflowPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='DataflowPythonOperator',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:

    # Task: start
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Dataflow batch job  log process task
    dataflow_batch_process_logs = DataFlowPythonOperator(
        task_id='dataflow_batch_process_logs',
        py_file='gs://us-central1-data-engineerin-6d2f5807-bucket/scripts/dataflow_batch_log_process.py',
        options={
            'output': 'gs://data_eng_demos/output'
        },
        dataflow_default_options={
            'project': 'data-eng-demos19',
            "staging_location": "gs://data_eng_demos/staging",
            "temp_location": "gs://data_eng_demos/temp"
        },
        dag=dag)



    # Task: end
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # Task Dependency
    start >> dataflow_batch_process_logs >> end
