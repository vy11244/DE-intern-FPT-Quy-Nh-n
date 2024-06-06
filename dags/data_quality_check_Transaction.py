"""
DAG for extracting data from ADLS to Bronze layer in Snowflake.
This DAG extracts data from an Azure Data Lake Storage account and loads it into the Bronze layer in Snowflake.
Note: Update the configuration parameters in the Variable section.

Warning: the load_blob_into_bronze.sql is crucial for loading the table into Snowflake, DO NOT DELETE THE FILE.
"""

import pendulum

from airflow import DAG

from dataUtils import DataQualityChecker

database_name = "TRANSACTION"
schema_name = "TRANSACTION_BRONZE"

data_checker = DataQualityChecker(database_name, schema_name)

with DAG(
    dag_id=f'data_check_{database_name}.{schema_name}',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    initialize_task = data_checker.initialize_rules(dag=dag)
    check_length = data_checker.create_rule_length(
        dag=dag, table_name='SUPPLIERTRANSACTIONS', col_name='TRANSACTIONDATE', min_len=0, max_len=5
    )
    
    initialize_task >> check_length