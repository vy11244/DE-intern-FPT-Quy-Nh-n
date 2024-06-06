from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from dataUtils.handler.snowflake_handler import SnowflakeHandler
from dataUtils.basic.task_creation import create_logging_gate_with_cleanup, create_get_latest_task_etl_table
from dataUtils.basic.table_utils import TableUtils

class DataLoader:
    """
    Extracts data from lake into data warehouse
    """
    def __init__(
        self,
        handler: object,
    ):
        """
        Initializes the DataLoader with a handler object.

        Args:
            handler (object): The handler object for data extraction.
        """
        self.handler = handler
        
    def create_blob_snowflake_dag_task(self, dag, table_lake: str, table_target:str,  module_name: str, target_primary_key: str,
                                    method: str, json_file_name: str):      
        if isinstance(self.handler, SnowflakeHandler):    
            get_latest_task = create_get_latest_task_etl_table(dag=dag,
                                                               source_name=table_lake,
                                                               task_type="Extract Source into Lake")
            
            create_query_task = PythonOperator(
                task_id=f'create_{table_target}_snowflake_query',
                python_callable=self.handler.create_extract_query,
                op_kwargs={
                    "table_lake":table_lake,
                    "table_target":table_target,
                    "module_name":module_name,
                    "target_primary_key":target_primary_key,
                    "method":method,
                    "json_file_name":json_file_name,
                    "latest_date":"{{ task_instance.xcom_pull(task_ids='" + f'get_latest_{TableUtils.check_table_name(table_lake)}' + "') }}",
                },
                provide_context=True
            )
                              
            run_query_task = SnowflakeOperator(
                task_id=f"extract_{table_target}_to_bronze_direct",
                sql="{{ task_instance.xcom_pull(task_ids='" + f'create_{table_target}_snowflake_query' + "') }}",
                dag=dag,
                snowflake_conn_id=self.handler.snowflake_conn_id
            )
            
            run_logging_task = create_logging_gate_with_cleanup(dag=dag,
                                                   source=table_lake,
                                                   target=table_target,
                                                   task_type="Load Lake into Bronze")
            
            get_latest_task >> create_query_task >> run_query_task >> run_logging_task
            
        else:
            raise AirflowException('Handler is not a SnowflakeHandler object')