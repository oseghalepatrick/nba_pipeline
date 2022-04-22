import os
import logging
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

from datetime import datetime, date

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from google.cloud import storage

import pyarrow.csv as pv
import pyarrow.parquet as pq

from playerInfo import scrapePlayersInfo
from playerSalary import scrapeSalary
from coaches import scrapeCoaches
from games import scrapeGames
from playerByPlay import scrapePlayByPlay
from boxScores import scrapeBoxScores

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'all_nba_data')

def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

def once_scrape_parquetize_upload_dag(
    dag,
    scrape_function,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        scrape_dataset_task = PythonOperator(
            task_id="scrape_dataset_task",
            python_callable=scrape_function,
            op_kwargs={
                "dest_file": local_csv_path_template
            },
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

def scrape_games_parquetize_upload_dag(
    dag,
    scrape_function,
    scrape_yr,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        scrape_dataset_task = PythonOperator(
            task_id="scrape_dataset_task",
            python_callable=scrape_function,
            op_kwargs={
                "year": scrape_yr,
                "dest_file": local_csv_path_template
            },
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task

def scrape_games_details_parquetize_upload_dag(
    dag,
    scrape_function,
    src_dir,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template
):
    with dag:
        scrape_dataset_task = PythonOperator(
            task_id="scrape_dataset_task",
            python_callable=scrape_function,
            op_kwargs={
                "src_file": src_dir,
                "dest_file": local_csv_path_template
            },
        )

        format_to_parquet_task = PythonOperator(
            task_id="format_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": local_csv_path_template,
                "dest_file": local_parquet_path_template
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_parquet_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template} {local_parquet_path_template}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

def rm_data_dag(
    dag,
    games_dir
):
    with dag:
        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {games_dir}"
        )

        rm_task
