from datetime import timedelta, datetime 


from airflow import DAG as DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from dags.funtions.SQL_extract import extr_Sql 

default_args = {
    "owner": "wmurillo",
    "depends_on_past":False,
    "email": ["murillowilmar1@gmail.com"], 
    "email_on_failure": False,
    "email_on_retry": False,  
    "retries": 5, 
    "retry_delay":timedelta(minutes=1)
}

with DAG(
     "SQL_DAG", 
     default_args=default_args,
     start_date= datetime(2022,1,12),
     max_active_runs =5, 
     description = "Move_DAG",
     schedule_interval="@hourly", 
     tags=["SQL"], 
     catchup =False, 
     template_searchpath = "/usr/local/air_flow/include/"
) as dag: 


    extract_data= PythonOperator(task_id="extract_data", python_callable=extr_Sql )
    Create_table_postg = PostgresOperator(
                         task_id = "Create_backup", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql"


    )






    extract_data >> Create_table_postg 