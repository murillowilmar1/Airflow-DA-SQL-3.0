from datetime import timedelta, datetime 


from airflow import DAG as DAG 
from airflow.utils.task_group import TaskGroup 
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator 
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


   # extract_data= PythonOperator(task_id="extract_data", python_callable=extr_Sql )
    start = EmptyOperator(task_id ="start")
    
    # Group task # 1 --  create tables

    with TaskGroup(group_id = "create_tables") as tg1: 

         Create_table_postg = PostgresOperator(
                         task_id = "Create_table_Pedidos", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         Create_table_postg1 = PostgresOperator(
                         task_id = "Create_table_Productos", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         Create_table_postg2 = PostgresOperator(
                         task_id = "Create_table_Pred", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         Create_table_postg3 = PostgresOperator(
                         task_id = "Create_table_Priey", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         


           # Segundo  grupo 

    with TaskGroup(group_id = "Extrac_info_tables") as tg2: 

         load_table_postg = PostgresOperator(
                         task_id = "Extrac_table_Pedidos", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         load_table_postg1 = PostgresOperator(
                         task_id = "Extrac_table_Productos", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         load_table_postg2 = PostgresOperator(
                         task_id = "Extrac_table_Pred", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         load_table_postg3 = PostgresOperator(
                         task_id = "Extrac_table_Priey", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")




   # Tercer  grupo 

    with TaskGroup(group_id = "Load_tables") as tg3: 

         load_table_postg = PostgresOperator(
                         task_id = "load_table_Pedidos", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         load_table_postg1 = PostgresOperator(
                         task_id = "load_table_Productos", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         load_table_postg2 = PostgresOperator(
                         task_id = "load_table_Pred", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")
         load_table_postg3 = PostgresOperator(
                         task_id = "load_table_Priey", 
                         postgres_conn_id ="Postgres_conn", 
                         sql = "sql/post.sql")



    End = EmptyOperator(task_id ="End")
    start>> tg1 >> tg2 >>tg3 >>End