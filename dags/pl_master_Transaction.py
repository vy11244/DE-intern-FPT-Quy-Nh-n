from airflow import DAG
from dataUtils import PipelineMaker

import pendulum

module_name = "Transaction"
pipeline_maker = PipelineMaker()

lake_list = ['pl_lake_Purchasing.Suppliers_extract','pl_lake_Application.People_extract','pl_lake_Sales.BuyingGroups_extract','pl_lake_Sales.CustomerCategories_extract'
             ,'pl_lake_Purchasing.SupplierCategories_extract','pl_lake_Sales.CustomerTransactions_extract','pl_lake_Application.PaymentMethods_extract'
             ,'pl_lake_Purchasing.SupplierTransactions_extract','pl_lake_Application.TransactionTypes_extract','pl_lake_Sales.Customers_extract']

bronze_list = ['pl_bronze_Transaction.Suppliers_load','pl_bronze_Transaction.CustomerCategories_load','pl_bronze_Transaction.Customers_load','pl_bronze_Transaction.BuyingGroups_load'
               ,'pl_bronze_Transaction.CustomerTransactions_load','pl_bronze_Transaction.PaymentMethod_load','pl_bronze_Transaction.People_load'
               ,'pl_bronze_Transaction.SupplierCategories_load','pl_bronze_Transaction.SupplierTransactions_load','pl_bronze_Transaction.TransactionTypes_load']

silver_list = ['pl_silver_Transaction.Suppliers_transform', 'pl_silver_Transaction.PaymentMethod_transform','pl_silver_Transaction.Customer_transform'
               ,'pl_silver_Transaction.Date_transform','pl_silver_Transaction.TransactionType_transform']

fact_list = ['pl_silver_Transaction.Transaction_transform']

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
