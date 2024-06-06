"""
DAG for transforming data from Bronze to Silver, focusing only on using query"
"""

import pendulum

from airflow import DAG

from dataUtils import SqlFileHandler, DataTransformer

#Folder that contains wanted tables from blob
table_target = "StockHolding.StockItem"
sql_file_name = "StockHolding.StockItem_dim.sql"
#Upload method is either insert, upsert or replace

sql_file_handler = SqlFileHandler()
data_transformer = DataTransformer(sql_file_handler=sql_file_handler)

with DAG(
    dag_id=f'pl_silver_{table_target}_transform',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    get_blob_packet_task = data_transformer.create_snowflake_query_task(dag, table_target, sql_file_name)