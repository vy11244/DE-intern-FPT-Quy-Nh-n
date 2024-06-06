from airflow import DAG
from dataUtils import PipelineMaker

import pendulum

module_name = "Sale"
pipeline_maker = PipelineMaker()

lake_list = ['pl_lake_Sales.BuyingGroups_extract','pl_lake_Application.Cities_extract','pl_lake_Sales.Customers_extract'
             ,'pl_lake_Sales.CustomerCategories_extract','pl_lake_Warehouse.Colors_extract','pl_lake_Application.Countries_extract'
             ,'pl_lake_Warehouse.PackageTypes_extract','pl_lake_Application.People_extract','pl_lake_Application.StateProvinces_extract'
             ,'pl_lake_Warehouse.StockItems_extract','pl_lake_Sales.Invoices_extract','pl_lake_Sales.InvoiceLines_extract']

bronze_list = ['pl_bronze_SALE.BUYINGGROUPS_load','pl_bronze_Sale.Cities_load','pl_bronze_Sale.Colors_load'
               ,'pl_bronze_Sale.Countries_load','pl_bronze_SALE.CUSTOMERCATEGORIES_load','pl_bronze_SALE.CUSTOMERS_load'
               ,'pl_bronze_Sale.PackageTypes_load','pl_bronze_Sale.People_load','pl_bronze_Sale.StateProvinces_load'
               ,'pl_bronze_Sale.StockItems_load', 'pl_bronze_Sale.InvoiceLines_load','pl_bronze_Sale.Invoices_load']

silver_list = ['pl_silver_Sale.City_transform', 'pl_silver_Sales.Customers_transform','pl_silver_Sale.Date_transform'
               ,'pl_silver_Sale.Employee_transform','pl_silver_Sale.StockItem_transform']

fact_list = ['pl_silver_Sale.Sale_transform']

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
