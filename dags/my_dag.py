
#changed to dev1422

# Import statement
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

# Define yesterday value for setting up start for DAG
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG main definition
with DAG(dag_id='DataflowPythonOperator',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:
    # Dummy Start task
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Dataflow batch job  log process task
    dataflow_batch_process_logs = DataFlowPythonOperator(
        task_id='dataflow_batch_process_logs',
        py_file='gs://us-central1-data-engineerin-6d2f5807-bucket/scripts/dataflow_batch_log_process.py',
        options={
            'output': 'gs://data_transformed14'
        },
        dataflow_default_options={
            'project': 'data-eng-demos19',
            "staging_location": "gs://logs_exports14/staging",
            "temp_location": "gs://composer14/temp"
        },
        dag=dag)

    # Dummy end task
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

# Setting up Task dependencies using Airflow standard notations
start >> dataflow_batch_process_logs >> end