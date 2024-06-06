"""
DAG for extracting data from MSSQL to ADLS.
This DAG extracts data from a Microsoft Server SQL Database and push it into an Azure Blob Storage.
Note: Update the configuration parameters in the Variable section.
"""

import pendulum

from airflow import DAG

from dataUtils import BlobStorageHandler, SqlFileHandler, DataExtractor, MSSQLHandler

table_source = "Application.Countries"
query_file_name = "countries_query.sql"

blob_handler = BlobStorageHandler()
sql_handler = SqlFileHandler()
mssql_handler = MSSQLHandler(sql_handler)
data_extractor = DataExtractor(source=mssql_handler, target=blob_handler)

with DAG(
    dag_id=f'pl_lake_{table_source}_extract',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    extract_source_into_local_task = data_extractor.create_mssql_dag_task_local(dag, table_source, query_file_name)
    upload_local_into_blob_task = data_extractor.create_blob_dag_task_upload(dag, table_source, None)
    
    extract_source_into_local_task >> upload_local_into_blob_task