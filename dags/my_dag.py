import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
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
    dag_id='gcs_to_bq',
    catchup=False,
    schedule_interval=timedelta(days=1),
    default_args=default_args
) as dag:

    # Task: start
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )

    # Task: gcs_to_bq
    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='data_eng_demos',
        source_objects=['Alubee_Update.xlsx'],
        destination_project_dataset_table='data-eng-demos19.gcp_dataeng_demos.gcs_to_bq_table',
        schema_fields=[
            {'name': 'Project Health', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Allocated Resource', 'type': 'INT', 'mode': 'NULLABLE'},
            {'name': 'Project Lead', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Developers', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Client Feedback', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Project Insight & Deliverables', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Risk/Blockers', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'New Requirements', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        skip_leading_rows=1,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        dag=dag
    )

    # Task: bq_manipulation
    bq_manipulation = BigQueryOperator(
        task_id='bq_manipulation',
        use_legacy_sql=False,
        allow_large_results=True,
        sql="CREATE OR REPLACE TABLE gcp_dataeng_demos.bq_table_aggr AS \
             SELECT \
                    year,\
                    anzsic_descriptor,\
                    variable,\
                    source,\
                    SUM(data_value) as sum_data_value\
             FROM data-eng-demos19.gcp_dataeng_demos.gcs_to_bq_table \
             GROUP BY \
                    year,\
                    anzsic_descriptor,\
                    variable,\
                    source",
        dag=dag
    )

    # Task: end
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    # Task Dependency
    start >> gcs_to_bq >> bq_manipulation >> end
