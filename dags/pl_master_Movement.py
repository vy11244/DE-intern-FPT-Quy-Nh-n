from airflow import DAG
from dataUtils import PipelineMaker

import pendulum

module_name = "Movement"
pipeline_maker = PipelineMaker()

lake_list = ['pl_lake_Sales.BuyingGroups_extract','pl_lake_Warehouse.Colors_extract','pl_lake_Sales.CustomerCategories_extract'
             ,'pl_lake_Sales.Customers_extract','pl_lake_Warehouse.PackageTypes_extract','pl_lake_Application.People_extract'
             ,'pl_lake_Warehouse.StockItems_extract','pl_lake_Warehouse.StockItemTransactions_extract','pl_lake_Purchasing.SupplierCategories_extract'
             ,'pl_lake_Purchasing.Suppliers_extract' ,'pl_lake_Application.TransactionTypes_extract']

bronze_list = ['pl_bronze_Movement.BuyingGroups_load','pl_bronze_MOVEMENT.Colors_load','pl_bronze_Movement.CustomerCategories_load'
               ,'pl_bronze_Movement.Customers_load','pl_bronze_MOVEMENT.PackageTypes_load','pl_bronze_Movement.People_load'
               ,'pl_bronze_Movement.StockItems_load','pl_bronze_MOVEMENT.StockItemTransactions_load','pl_bronze_MOVEMENT.SupplierCategories_load'
               ,'pl_bronze_Movement.Suppliers_load','pl_bronze_Movement.TransactionTypes_load']

silver_list = ['pl_silver_Movement.StockItem_transform', 'pl_silver_Movement.Supplier_transform','pl_silver_Movement.TransactionType_transform'
               ,'pl_silver_Movement.Date_transform','pl_silver_Movement.Customer_transform']

fact_list = ['pl_silver_Movement.Movement_transform']

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
