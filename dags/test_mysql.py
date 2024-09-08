from airflow import DAG
from airflow.decorators import dag,task
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, date, timedelta
from airflow.utils.dates import days_ago
import os
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models.variable import Variable
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator

#GCP Variables
bucket              = "de-ark-storage"
gcs_project         = "mp4-arkademi"
gcs_connection      = "gcp_arkademi"
mysql_connection    = "local_mysql"
tables              = "kupon_list"
dataset             = "de_test"
file_ext            = "CSV"
resource_path       = ""




#dag arguments
default_args = {
    "owner" : "Kenta Edmonda",
    "depend_on_past" : False,
    "start_date" : days_ago(1),
    "email" : ["kenta.arkademi@gmail.com"],
    "email_on_failure" : True,
    "email_on_retry" : True,
    "councurrency" : 1,
    "retries" : 0,
    "retry_delay" : timedelta(minutes=1)
}

#define dag

dag = DAG('Mysql_GCP',
          schedule_interval='@once',
          default_args=default_args,
          description='Moving data from MySQL to GCP')


start_task = EmptyOperator(task_id='Start_task', dag=dag)
end_task = EmptyOperator(task_id='End_task', dag=dag)

extract = MySQLToGCSOperator(
    task_id             = "extract_data",
    export_format       = "csv", # untuk setting pengiriman berupa file csv (karena class ini secara default mengirim json)
    mysql_conn_id       = mysql_connection,
    gcp_conn_id         = gcs_connection,
    sql                 =  "select * from kupon_list", 
    bucket              = bucket,
    filename            = resource_path + tables + "." + file_ext,
    schema_filename     =  tables + "_schemas.json",
    dag = dag
)

load = GCSToBigQueryOperator(
    task_id             = "load_to_bq",
    gcp_conn_id         = gcs_connection,
    bucket              = bucket,
    destination_project_dataset_table="mp4-arkademi.de_test.table_testing", #fixed gcs to bigquery
    source_objects      = ["kupon_list.CSV"],
    source_format       = "CSV",
    field_delimiter     = ",",
    skip_leading_rows   = 1,
    max_bad_records     = 100,
    allow_quoted_newlines   = True,
    project_id          = gcs_project,
    ignore_unknown_values= True,
    autodetect          = True,
    create_disposition  = 'CREATE_IF_NEEDED',
    write_disposition   = 'WRITE_APPEND',
    schema_fields       = [{"mode": "NULLABLE", "name": "id", "type": "INTEGER"}, 
                           {"mode": "NULLABLE", "name": "prefix", "type": "STRING"}, 
                           {"mode": "NULLABLE", "name": "kategori", "type": "STRING"}, 
                           {"mode": "NULLABLE", "name": "channel", "type": "STRING"}],
    dag                 = dag

)



start_task >> extract >> load >> end_task