from airflow import DAG
from dataUtils import PipelineMaker

import pendulum

module_name = "Purchase"
pipeline_maker = PipelineMaker()

lake_list = ['pl_lake_Warehouse.Colors_extract','pl_lake_Warehouse.PackageTypes_extract','pl_lake_Application.People_extract'
             ,'pl_lake_Purchasing.PurchaseOrderLines_extract','pl_lake_Purchasing.PurchaseOrders_extract','pl_lake_Warehouse.StockItems_extract'
             ,'pl_lake_Purchasing.SupplierCategories_extract','pl_lake_Purchasing.Suppliers_extract']

bronze_list = ['pl_bronze_Purchase.Colors_load','pl_bronze_Purchase.People_load','pl_bronze_Purchase.PackageTypes_load'
               ,'pl_bronze_Purchase.PurchaseOrderLines_load','pl_bronze_Purchase.PurchaseOrders_load','pl_bronze_Purchase.StockItems_load'
               ,'pl_bronze_Purchase.SupplierCategories_load','pl_bronze_Purchase.Suppliers_load']

silver_list = ['pl_silver_Purchase.Date_transform', 'pl_silver_Purchase.StockItem_transform', 'pl_silver_Purchase.Supplier_transform']

fact_list = ['pl_silver_Purchase.Purchase_transform']

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
