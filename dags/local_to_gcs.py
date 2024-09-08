from airflow import DAG
from airflow.decorators import dag,task
# from airflow.operators.bash import BashOperator
# from airflow.operators.email import EmailOperator
# from airflow.operators.python import BranchPythonOperator, PythonOperator
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


import pandas as pd
import csv

from datetime import datetime
#link kaggle : https://www.kaggle.com/code/muhammadehsan02/starter-paris-olympic-2024
# Fungsi 1: Ekstrak data dari athlete.csv



BASE_PATH       = Variable.get("BASE_PATH")
DATASET_ID      = Variable.get("DATASET_ID")
BUCKET_NAME     = Variable.get("BUCKET_NAME")
GC_CONN_ID      = Variable.get("GC_CONN_ID")
BQ_TABLE_NAME   = ""
GCS_OBJECT_NAME = "extract_local.csv"
DATA_PATH       = f"{BASE_PATH}/data"

default_args =  {
    'owner' : 'Kenta',
    'email' : 'kentaedmonda01@gmail.com',
    'email_on_failure' : True,
    'retries' : 1,
    "depend_on_past" : False,
    "retry_delay" : timedelta(minutes=1)
}

@dag(   
    "local_olympics_to_gcs",
    default_args        = default_args,
    schedule_interval   = '@once',
    start_date          = days_ago(1),   
    catchup             = False,
    tags                = ['olympics', 'gcp', 'elt']
)

def elt_olympics_to_gcp():

    @task()
    def extract_and_transform_athletes():
        file_path = f"{DATA_PATH}athletes.csv"
        today = datetime.today()
        df_athletes = pd.read_csv(file_path)
        df_athletes['birth_date'] = pd.to_datetime(df_athletes['birth_date'])
        df_athletes['age'] = df_athletes['birth_date'].apply(lambda row : today.year - row.year - ((today.month, today.day) < (row.month, row.day)))
        df_athletes.to_csv(f"{DATA_PATH}/processed_athletes.csv", index=False)

        return f"{DATA_PATH}/processed_athletes.csv"
    
    @task()
    def extract_and_transform_events():
        file_path = f"{DATA_PATH}/events.csv"
        df_events = pd.read_csv(file_path)
        df_events.to_csv(f"{DATA_PATH}/processed_events.csv", index=False)

        return f"{DATA_PATH}/processed_events.csv"
        
    @task()
    def extract_and_transform_medals():
        file_path = f"{DATA_PATH}/medals.csv"
        df_medals = pd.read_csv(file_path)
        df_medals.to_csv(f"{DATA_PATH}/processed_medals.csv", index=False)

        return f"{DATA_PATH}/processed_medals.csv"
    
    @task()
    def extract_and_transform_schedules():
        file_path = f"{DATA_PATH}/schedules.csv"
        df_schedules = pd.read_csv(file_path)
        df_schedules['start_date'] = pd.to_datetime(df_schedules['start_date'])
        df_schedules['end_date'] = pd.to_datetime(df_schedules['end_date'])
        df_schedules['date'] = df_schedules['start_date'].dt.date
        df_schedules.to_csv(f"{DATA_PATH}/processed_schedules.csv", index=False)

        return f"{DATA_PATH}/processed_schedules.csv"
    
    @task()    
    def extract_and_transform_teams():
        file_path = f"{DATA_PATH}/teams.csv"
        df_teams = pd.read_csv(file_path)
        df_teams.to_csv(f"{DATA_PATH}/processed_teams.csv", index=False)

        return f"{DATA_PATH}/processed_teams.csv"
    
    # Extract and transform tasks
    athletes_data   = extract_and_transform_athletes()
    events_data     = extract_and_transform_events()
    medals_data     = extract_and_transform_medals()
    schedules_data  = extract_and_transform_schedules()
    teams_data      = extract_and_transform_teams()


    start_task = EmptyOperator(task_id="Start_task")
    end_task = EmptyOperator(task_id="End_Task")
    # Load to GCS 
    load_athletes_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "load_athletes_to_gcs",
        src = athletes_data,
        dst = "processed_athletes.csv",
        bucket = BUCKET_NAME,
        gcp_conn_id = GC_CONN_ID,
    )
    load_events_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "load_events_to_gcs",
        src = events_data,
        dst = "processed_events.csv",
        bucket = BUCKET_NAME,
        gcp_conn_id = GC_CONN_ID,
    )
    load_medals_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "load_medals_to_gcs",
        src = medals_data,
        dst = "processed_medals.csv",
        bucket = BUCKET_NAME,
        gcp_conn_id = GC_CONN_ID,
    )
    load_schedules_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "load_schedules_to_gcs",
        src = schedules_data,
        dst = "processed_schedules.csv",
        bucket = BUCKET_NAME,
        gcp_conn_id = GC_CONN_ID,
    )
    load_teams_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "load_teams_to_gcs",
        src = teams_data,
        dst = "processed_teams.csv",
        bucket = BUCKET_NAME,
        gcp_conn_id = GC_CONN_ID,
    )

    start_task >> [athletes_data, events_data, medals_data, schedules_data, teams_data]  # All extraction tasks follow start
    athletes_data >> load_athletes_to_gcs  # After extraction, load athletes data to GCS
    events_data >> load_events_to_gcs      # After extraction, load events data to GCS
    medals_data >> load_medals_to_gcs      # After extraction, load medals data to GCS
    schedules_data >> load_schedules_to_gcs  # After extraction, load schedules data to GCS
    teams_data >> load_teams_to_gcs        # After extraction, load teams data to GCS

    # All loading tasks should complete before the end task
    [load_athletes_to_gcs, load_events_to_gcs, load_medals_to_gcs, load_schedules_to_gcs, load_teams_to_gcs] >> end_task
    

        
elt_olympics_to_gcp()
  