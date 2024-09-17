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



BASE_PATH       = Variable.get("BASE_PATH")
DATASET_ID      = Variable.get("DATASET_ID")
BUCKET_NAME     = Variable.get("BUCKET_NAME")
GC_CONN_ID      = Variable.get("GC_CONN_ID")
GCP_PROJECT     = "agile-alignment-435006-h3"
BQ_TABLE_NAME   = "agile-alignment-435006-h3.EDM_Data"
GCS_OBJECT_NAME = "extract_local.csv"
DATA_PATH       = f"{BASE_PATH}/data"
athletes_schema = "athletes_schemas.json"        
events_schema   = "events_schemas.json"        
medals_schema   = "medals_schemas.json"        
schedules_schema = "schedules_schemas.json"        
teams_schema    = "athletes_schemas.json"        

default_args =  {
    'owner' : 'Kenta',
    'email' : 'kentaedmonda01@gmail.com',
    'email_on_failure' : False,
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
        file_path = f"{DATA_PATH}/athletes.csv"
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
    
    @task()    
    def extract_and_transform_total_medals():
        file_path = f"{DATA_PATH}/medals_total.csv"
        df_teams = pd.read_csv(file_path)
        df_teams.to_csv(f"{DATA_PATH}/processed_medals_total.csv", index=False)

        return f"{DATA_PATH}/processed_medals_total.csv"
    
    # Extract and transform tasks
    athletes_data   = extract_and_transform_athletes()
    events_data     = extract_and_transform_events()
    medals_data     = extract_and_transform_medals()
    schedules_data  = extract_and_transform_schedules()
    teams_data      = extract_and_transform_teams()
    medals_total    = extract_and_transform_total_medals()


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

    load_medals_total_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "load_medals_total_to_gcs",
        src = medals_total,
        dst = "processed_medals_total.csv",
        bucket = BUCKET_NAME,
        gcp_conn_id = GC_CONN_ID,
    )


    # GCS To Bigquery

    load_athletes_to_bigquery = GCSToBigQueryOperator(
        task_id         = "load_athletes_to_bigquery",
        bucket          = BUCKET_NAME,
        gcp_conn_id     = GC_CONN_ID,
        source_objects  = ['processed_athletes.csv'],
        destination_project_dataset_table = f'{BQ_TABLE_NAME}.table_athletes',
        source_format   = 'csv',
        field_delimiter = ',',
        skip_leading_rows = 1,
        max_bad_records = 100,
        project_id      = GCP_PROJECT,
        autodetect      = True,
        ignore_unknown_values = True,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND",
        schema_fields   = [
                            {"mode": "REQUIRED", "name": "code", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "name", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "name_short", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "name_tv", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "gender", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "function", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country_long", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "nationality", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "nationality_full", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "nationality_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "height", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "weight", "type": "FLOAT"},
                            {"mode": "NULLABLE", "name": "disciplines", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "events", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "birth_date", "type": "DATE"},
                            {"mode": "NULLABLE", "name": "birth_place", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "birth_country", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "residence_place", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "residence_country", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "nickname", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "hobbies", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "occupation", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "education", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "family", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "lang", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "coach", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "reason", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "hero", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "influence", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "philosophy", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "sporting_relatives", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "ritual", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "other_sports", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "age", "type": "INTEGER"}
                        ]

    )
    load_events_to_bigquery = GCSToBigQueryOperator(
        task_id         = "load_events_to_bigquery",
        bucket          = BUCKET_NAME,
        gcp_conn_id     = GC_CONN_ID,
        source_objects  = ['processed_events.csv'],
        destination_project_dataset_table = f'{BQ_TABLE_NAME}.table_events',
        source_format   = 'csv',
        field_delimiter = ',',
        skip_leading_rows = 1,
        max_bad_records = 100,
        project_id      = GCP_PROJECT,
        autodetect      = True,
        ignore_unknown_values = True,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND",
        schema_fields   = [
                            {"mode": "REQUIRED", "name": "event", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "tag", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "sport", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "sport_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "sport_url", "type": "STRING"}
                        ]

    )
    load_medals_to_bigquery = GCSToBigQueryOperator(
        task_id         = "load_medals_to_bigquery",
        bucket          = BUCKET_NAME,
        gcp_conn_id     = GC_CONN_ID,
        source_objects  = ['processed_medals.csv'],
        destination_project_dataset_table = f'{BQ_TABLE_NAME}.table_medals',
        source_format   = 'csv',
        field_delimiter = ',',
        skip_leading_rows = 1,
        max_bad_records = 100,
        project_id      = GCP_PROJECT,
        autodetect      = True,
        ignore_unknown_values = True,
        allow_quoted_newlines = True,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND",
        schema_fields   = [
                            {"mode": "REQUIRED", "name": "medal_type", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "medal_code", "type": "FLOAT"},
                            {"mode": "REQUIRED", "name": "medal_date", "type": "DATE"},
                            {"mode": "REQUIRED", "name": "name", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "gender", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "discipline", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "event", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "event_type", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "url_event", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "code", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "country_code", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "country", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "country_long", "type": "STRING"}
                        ]

    )
    load_schedules_to_bigquery = GCSToBigQueryOperator(
        task_id         = "load_schedules_to_bigquery",
        bucket          = BUCKET_NAME,
        gcp_conn_id     = GC_CONN_ID,
        source_objects  = ['processed_schedules.csv'],
        destination_project_dataset_table = f'{BQ_TABLE_NAME}.table_schedules',
        source_format   = 'csv',
        field_delimiter = ',',
        skip_leading_rows = 1,
        max_bad_records = 100,
        project_id      = GCP_PROJECT,
        autodetect      = True,
        ignore_unknown_values = True,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND",
        schema_fields   = [
                            {"mode": "NULLABLE", "name": "start_date", "type": "TIMESTAMP"},
                            {"mode": "NULLABLE", "name": "end_date", "type": "TIMESTAMP"},
                            {"mode": "NULLABLE", "name": "day", "type": "DATE"},
                            {"mode": "NULLABLE", "name": "status", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "discipline", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "discipline_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "event", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "event_medal", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "phase", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "gender", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "event_type", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "venue", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "venue_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "location_description", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "location_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "url", "type": "STRING"}
                        ]

    )
    load_teams_to_bigquery = GCSToBigQueryOperator(
        task_id         = "load_teams_to_bigquery",
        bucket          = BUCKET_NAME,
        gcp_conn_id     = GC_CONN_ID,
        source_objects  = ['processed_teams.csv'],
        destination_project_dataset_table = f'{BQ_TABLE_NAME}.table_teams',
        source_format   = 'csv',
        field_delimiter = ',',
        skip_leading_rows = 1,
        max_bad_records = 0,
        project_id      = GCP_PROJECT,
        autodetect      = True,
        ignore_unknown_values = True,
        allow_quoted_newlines = True,
         quote_character = '"',
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND",
        schema_fields   = [
                            {"mode": "NULLABLE", "name": "code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "team", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "team_gender", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country_long", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "discipline", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "disciplines_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "events", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "athletes", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "coaches", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "athletes_codes", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "num_athletes", "type": "FLOAT"},
                            {"mode": "NULLABLE", "name": "coaches_codes", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "num_coaches", "type": "FLOAT"}
                        ]

    )

    load_medals_total_to_bigquery = GCSToBigQueryOperator(
        task_id         = "load_medals_total_to_bigquery",
        bucket          = BUCKET_NAME,
        gcp_conn_id     = GC_CONN_ID,
        source_objects  = ['processed_medals_total.csv'],
        destination_project_dataset_table = f'{BQ_TABLE_NAME}.table_medals_total',
        source_format   = 'csv',
        field_delimiter = ',',
        skip_leading_rows = 1,
        max_bad_records = 0,
        project_id      = GCP_PROJECT,
        autodetect      = True,
        ignore_unknown_values = True,
        allow_quoted_newlines = True,
         quote_character = '"',
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = "WRITE_APPEND",
        schema_fields   = [
                            {"mode": "NULLABLE", "name": "country_code", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "country_long", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "Gold_Medal", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "Silver_Medal", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "Bronze_Medal", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "Total", "type": "INTEGER"}
                            ]
    )
    start_task >> [athletes_data, events_data, medals_data, schedules_data, teams_data, medals_total]  # All extraction tasks follow start
    athletes_data >> load_athletes_to_gcs  # After extraction, load athletes data to GCS
    events_data >> load_events_to_gcs      # After extraction, load events data to GCS
    medals_data >> load_medals_to_gcs      # After extraction, load medals data to GCS
    schedules_data >> load_schedules_to_gcs  # After extraction, load schedules data to GCS
    teams_data >> load_teams_to_gcs        # After extraction, load teams data to GCS
    medals_total >> load_medals_total_to_gcs

    # All loading tasks should complete before the end task
    load_athletes_to_gcs >> load_athletes_to_bigquery
    load_events_to_gcs >> load_events_to_bigquery
    load_medals_to_gcs >> load_medals_to_bigquery
    load_schedules_to_gcs >> load_schedules_to_bigquery
    load_teams_to_gcs >> load_teams_to_bigquery
    load_medals_total_to_gcs >> load_medals_total_to_bigquery

    [load_athletes_to_bigquery, load_events_to_bigquery, load_medals_to_bigquery, load_schedules_to_bigquery, load_teams_to_bigquery, load_medals_total_to_bigquery] >> end_task
    

        
elt_olympics_to_gcp()
  