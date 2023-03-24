import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
DATASET_FILE = 'yellow_tripdata_2019-01.csv.gz'
DATASET_URL = URL_PREFIX + DATASET_FILE

PATH_TO_LOCAL_HOME = os.environ.get("AIRFLOW_HOME", 'opt/airflow')
parquet_file = DATASET_FILE.replace('.csv.gz', '.parquet')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'trips_data_all')

def format_to_parquet(src_file):
    logging.info(f"file name: {src_file}")
    if not src_file.endswith('.csv.gz'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    """

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    logging.info(f'Loading GCS bucket, {client}')
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    logging.info(f'Uploaded Files to GCS bucket {bucket}')


default_args = {
    'owner':'airflow',
    'start_date':days_ago(1),   
    'depends_on_past':False,
    #'retries':1,
}

with DAG(
    dag_id = 'data_ingestion_gcs_dag',
    schedule_interval = '@daily',
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
    tags = ['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id = 'download_dataset_task',
        bash_command = f'curl -sSL {DATASET_URL} > {PATH_TO_LOCAL_HOME}/{DATASET_FILE}'
    )

    format_to_parquet = PythonOperator(
        task_id = 'format_to_parquet',
        python_callable = format_to_parquet,
        op_kwargs = {
            'src_file': f"{PATH_TO_LOCAL_HOME}/{DATASET_FILE}"
            }
    )

    local_to_gcs= PythonOperator(
        task_id = 'local_to_gcs',
        python_callable = upload_to_gcs,
        op_kwargs = {
            'bucket': BUCKET,
            'object_name': f"raw/{parquet_file}",
            'local_file': f"{PATH_TO_LOCAL_HOME}/{parquet_file}"
        }
    )

    biqquery_external_table = BigQueryCreateExternalTableOperator(
        task_id = 'biqquery_external_table',
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet >> local_to_gcs >> biqquery_external_table