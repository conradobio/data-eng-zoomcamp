import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_GCS_BUCKET")
path_to_local_home = os.getenv("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", 'trips_data_all')

DATASET = "tripdata"
TAXI_TYPES = {'yellow': 'tpep_pickup_datetime', 'fhv': 'Pickup_datetime', 'green': 'lpep_pickup_datetime'}
INPUT_PART = "raw"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for taxi_type, ds_col in TAXI_TYPES.items():
        gcs_to_gcs = GCSToGCSOperator(
            task_id=f"move_{taxi_type}_{DATASET}_files",
            source_bucket=BUCKET,
            source_object=f'{INPUT_PART}/{taxi_type}/*',
            destination_bucket=BUCKET,
            destination_object=f'{taxi_type}/{taxi_type}/',
            move_object=False,
        )

        gcs_to_bq = BigQueryCreateExternalTableOperator(
            task_id=f"move_{taxi_type}_{DATASET}_external_table",
            table_resource={
                'tableReference':{
                    'projectId': PROJECT_ID,
                    'datasetId': BIGQUERY_DATASET,
                    'tableId': f'{taxi_type}_{DATASET}_external_table',
                },
                'externalDataConfiguration':{
                    'autodetec': 'True',
                    'sourceFormat': 'PARQUET',
                    'sourceUris': [f"gs://{BUCKET}/{taxi_type}/*"]
                },
            },
        )

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{taxi_type}_{DATASET} \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{taxi_type}_{DATASET}_external_table;"
        )

        bq_ext_2_part = BigQueryInsertJobOperator(
            task_id=f"bq_create_{taxi_type}_{DATASET}_partitioned_table",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        gcs_to_gcs >> gcs_to_bq >> bq_ext_2_part
