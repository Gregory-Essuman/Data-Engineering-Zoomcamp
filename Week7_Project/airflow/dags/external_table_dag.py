import os
import glob

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'airline_delays')
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/data' + '/*.csv'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="external_table_dag",
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['gregg-airline'],
) as dag:

    copy_files_gcs_task = GCSToGCSOperator(
        task_id="copy_files_gcs_task",
        source_bucket=BUCKET,
        source_object="raw//opt/airflow/data*.parquet",
        destination_bucket=BUCKET,
        destination_object="new_raw",
        #move_object=True
        )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "airline_external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "autodetect": "True",
                "sourceUris": [f"gs://{BUCKET}/new_raw/*.parquet"],
            },
        },
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.airline_partitioned_clustered` \
        PARTITION BY RANGE_BUCKET(Month, GENERATE_ARRAY(1, 13)) \
        CLUSTER BY FlightNum AS \
        SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.airline_external_table`;"
    )

    # Create a partitioned and clustered table from external table
    bq_create_partitioned_clustered_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_create_partitioned_clustered_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    copy_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_clustered_table_task