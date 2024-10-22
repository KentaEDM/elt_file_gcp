import pandas as pd
import requests as req
import json

from airflow.decorators import dag,task
from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago
# import os
# from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.models.variable import Variable
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.models.variable import Variable


BASE_PATH       = Variable.get("BASE_PATH")
DATASET_ID      = Variable.get("DATASET_ID")
BUCKET_NAME     = Variable.get("BUCKET_NAME")
GC_CONN_ID      = Variable.get("GC_CONN_ID")
BQ_TABLE_NAME   = "agile-alignment-435006-h3.EDM_Data"
DATA_PATH       = f"{BASE_PATH}/data"
GCP_PROJECT     = "agile-alignment-435006-h3"

default_args = {
    'owner' : 'Kenta EDM',
    'email' : 'kenta03@gmail.com',
    'email_on_failure' : False,
    'retries' : 1,
    'depends_on_past' : False,
    'retry_delay' : timedelta(minutes=1)
}


@dag(
        "api_to_gcp",
        default_args = default_args,
        schedule_interval =  '@once',
        start_date = days_ago(1),
        catchup = False,
        tags    = ['API', 'ELT', 'GCP']
)

def api_to_gcp():
    @task()
    def api_extract():

        col = ['id' , 'symbol' , 'name', 'image', 'current_price', 'market_cap', 
               'market_cap_rank', 'total_volume', 'price_change_24h', 'total_supply', 'max_supply', 'ath_date', 'atl_date','last_updated']
        
        API = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page=i&sparkline=false"
        requests = req.get(API)
        data = requests.json()
        df = pd.DataFrame(data)
        df = df[col]

        return df
    
    @task()
    def transform_api(data):

        df = data.copy()
        df['last_updated'] = pd.to_datetime(df['last_updated'])
        df['atl_date'] = pd.to_datetime(df['atl_date'])
        df['day_update'] = df['last_updated'].dt.day_name()

        df.to_csv(f'{DATA_PATH}/api_data.csv', index = False)

        return f"{DATA_PATH}/api_data.csv"

    extract = api_extract()
    transform = transform_api(extract)

    start_task  = EmptyOperator(task_id='start_task')
    done_task   = EmptyOperator(task_id='end_task')

    load_to_gcs = LocalFilesystemToGCSOperator(
            task_id = "load_to_gcs",
            bucket  = BUCKET_NAME,
            src     = transform,
            dst     = "processed_api_data.csv",
            gcp_conn_id = GC_CONN_ID
    )
    load_to_bq  = GCSToBigQueryOperator(
            task_id         = "load_data_gcs_to_bq",
            source_objects  = ['processed_api_data.csv'],
            bucket          = BUCKET_NAME,
            destination_project_dataset_table = f"{BQ_TABLE_NAME}.processed_api",
            source_format   = 'csv',
            gcp_conn_id     = GC_CONN_ID,
            field_delimiter = ',',
            skip_leading_rows = 1,
            project_id      = GCP_PROJECT,
            autodetect      = True,
            max_bad_records = 100,
            ignore_unknown_values = True,
            create_disposition = "CREATE_IF_NEEDED",
            write_disposition = "WRITE_APPEND"
            # schema_fields   = [{}]
    )

    start_task >> extract >> transform >> load_to_gcs >> load_to_bq >> done_task


api_to_gcp()
