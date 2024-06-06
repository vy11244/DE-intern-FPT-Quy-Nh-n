from airflow import DAG
from dataUtils import PipelineMaker

import pendulum

module_name = "StockHolding"
pipeline_maker = PipelineMaker()

lake_list = ['pl_lake_Warehouse.Colors_extract','pl_lake_Warehouse.PackageTypes_extract','pl_lake_Warehouse.StockItems_extract'
             ,'pl_lake_Warehouse.StockItemHoldings_extract']

bronze_list = ['pl_bronze_StockHolding.Colors_load','pl_bronze_StockHolding.PackageTypes_load','pl_bronze_StockHolding.StockItems_load'
               ,'pl_bronze_StockHolding.StockItemHoldings_load']

silver_list = ['pl_silver_StockHolding.StockItem_transform']

fact_list = ['pl_silver_StockHolding.StockHolding_transform']

task_all_list = [lake_list, bronze_list, silver_list, fact_list]

with DAG(
    
    dag_id=f'pl_master_{module_name}',
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    
) as dag:
    pipeline_maker.create_master_pipeline(
        dag=dag,
        task_all=task_all_list,
        module_name=module_name
    )
