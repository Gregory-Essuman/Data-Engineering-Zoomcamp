import os
import glob
import logging

import pyarrow.csv as pv
import pyarrow.parquet as pq

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'airline_delays')
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/data' + '/*.csv'

def format_to_parquet():
    for filename in glob.iglob(OUTPUT_FILE_TEMPLATE):
        if not filename.endswith('.csv'):
            logging.error("Can only accept source files in CSV format, for the moment")
            return
        table = pv.read_csv(filename)
        pq.write_table(table, filename.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket_name):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket_name: GCS bucket name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    for parquet_file in glob.iglob("/opt/airflow/data/*.parquet"):

        object_name = f"new_raw{parquet_file}"
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        blob = bucket.blob(object_name)
        blob.upload_from_filename(parquet_file)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['gregg-airline'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command="/opt/airflow/scripts/list.sh "
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket_name": BUCKET
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task