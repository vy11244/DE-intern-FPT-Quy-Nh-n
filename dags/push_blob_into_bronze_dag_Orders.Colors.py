"""
DAG for extracting data from ADLS to Bronze layer in Snowflake.
This DAG extracts data from an Azure Data Lake Storage account and loads it into the Bronze layer in Snowflake.
Note: Update the configuration parameters in the Variable section.

Warning: the load_blob_into_bronze.sql is crucial for loading the table into Snowflake, DO NOT DELETE THE FILE.
"""

import pendulum

from airflow import DAG

from dataUtils import JsonFileHandler, BlobStorageHandler, SnowflakeHandler, DataLoader

table_lake = "Colors" #Folder that contains wanted tables from blob
table_target = "Orders.Colors"
module_name = "ORDERS"
target_primary_key = "COLORID"

upload_method = "replace"
#Upload method is either insert, upsert or replace
json_file_name = "Orders.color_mapping.json"
#Use a json file in JSON_PATH_FILE to map columns, only use when upserting into table

json_file_handler = JsonFileHandler()
blob_handler = BlobStorageHandler()
snowflake_handler = SnowflakeHandler(blob_handler, json_file_handler)
data_loader = DataLoader(handler=snowflake_handler)

with DAG(
    dag_id=f'pl_bronze_{table_target}_load',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    get_blob_packet_task = data_loader.create_blob_snowflake_dag_task(
        dag=dag,
        table_lake=table_lake,
        table_target=table_target,
        module_name=module_name,
        target_primary_key=target_primary_key,
        method=upload_method,
        json_file_name=json_file_name,
    )