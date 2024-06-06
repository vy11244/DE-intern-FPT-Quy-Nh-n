from airflow import DAG
from dataUtils import PipelineMaker

import pendulum

module_name = "Orders"
pipeline_maker = PipelineMaker()

lake_list = ['pl_lake_Sales.BuyingGroups_extract','pl_lake_Application.People_extract','pl_lake_Application.StateProvinces_extract'
             ,'pl_lake_Application.Cities_extract','pl_lake_Warehouse.Colors_extract','pl_lake_Application.Countries_extract'
             ,'pl_lake_Sales.CustomerCategories_extract','pl_lake_Sales.Customers_extract','pl_lake_Sales.OrderLines_extract'
             ,'pl_lake_Sales.Orders_extract','pl_lake_Warehouse.PackageTypes_extract','pl_lake_Warehouse.StockItems_extract']

bronze_list = ['pl_bronze_ORDERS.BuyingGroups_load','pl_bronze_ORDERS.Cities_load','pl_bronze_Orders.Colors_load'
               ,'pl_bronze_ORDERS.Countries_load','pl_bronze_ORDERS.CustomerCategories_load','pl_bronze_ORDERS.Customers_load'
               ,'pl_bronze_ORDERS.OrderLines_load' ,'pl_bronze_ORDERS.Orders_load','pl_bronze_Orders.Packagetypes_load'
               ,'pl_bronze_Orders.People_load','pl_bronze_ORDERS.StateProvinces_load','pl_bronze_Orders.StockItems_load']

silver_list = ['pl_silver_Orders.City_transform', 'pl_silver_Orders.Customer_transform','pl_silver_Orders.Employee_transform'
               ,'pl_silver_Orders.StockItem_transform','pl_silver_ORDERS.Date_transform']

fact_list = ['pl_silver_ORDERS.Orders_transform']

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
