from airflow.operators.mssql_operator import MsSqlOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator



def extr_Sql():
   #  logging.info("scrapping")
   mssql_hook = MsSqlHook(mssql_conn_id="SQL_server_conn", schema="Norte")
   with open ("/usr/local/airflow/include/Norte.sql", "r") as sqlfile:
    sql_stm= sqlfile.read()
   df = mssql_hook.get_pandas_df(sql = f"{sql_stm}") 
   df.to_csv("/usr/local/airflow/tests/Norte.sql.csv")