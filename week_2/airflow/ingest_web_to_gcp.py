import io
import os
from dotenv import load_dotenv
import requests
import pandas as pd
import logging
import pyarrow
from google.cloud import storage
from oauth2client.service_account import ServiceAccountCredentials
import json

load_dotenv()

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')

URL_PREFIX = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'


def upload_to_gcs(bucket, object_name, local_file):
    """
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    """

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client.from_service_account_json(json_credentials_path='/home/conrado/.gc/credentials/google_credentials.json')
    print(f'Loading GCS bucket, {client}')
    bucket = client.bucket(bucket)

    blob = bucket.blob(f'raw/{object_name}')
    blob.upload_from_filename(local_file)
    print(f'Uploaded Files to GCS bucket {bucket}')

def pull_data_to_gcp(year, service):

    for i in range(12):
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'

        # download it using requests via a pandas df
        request_url = URL_PREFIX + file_name

        r = requests.get(request_url)
        #/home/conrado/repos/Pessoal/data-eng-zoocamp/week_2/airflow/data
        pd.DataFrame(io.StringIO(r.text)).to_csv(f'week_2/airflow/data/{file_name}')
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(f'week_2/airflow/data/{file_name}')
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(f'week_2/airflow/data/{file_name}', engine='pyarrow')
        print(f"Parquet: {file_name}")

        print(BUCKET)
        upload_to_gcs(BUCKET, f"{service}/{file_name}", f'week_2/airflow/data/{file_name}')

def pull_data_to_bigquery():
    pass

if __name__ == "__main__":

    services = ['green','yellow', 'fhv']
    years = ['2019', '2020']

    for service in services:
        for year in years:
            print(f'Uploading data from {service} - {year}')
            pull_data_to_gcp(year, service)

