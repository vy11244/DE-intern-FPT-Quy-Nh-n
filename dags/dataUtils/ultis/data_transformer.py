from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from dataUtils.handler.sql_file_handler import SqlFileHandler
from dataUtils.basic.config_file import SNOWFLAKE_CONN_ID
from dataUtils.basic.task_creation import create_logging_gate

class DataTransformer:
    """
    Extracts data from lake into data warehouse
    """
    def __init__(
        self,
        sql_file_handler: SqlFileHandler
    ):
        self.sql_file_handler = sql_file_handler
    def create_snowflake_query_task(self, dag, table_target: str, query_file_name: str, snowflake_conn_id: str=SNOWFLAKE_CONN_ID):
        transform_task = SnowflakeOperator(
            task_id=f"transform_{table_target}_in_data_warehouse",
            sql=self.sql_file_handler.read_sql_into_string(table_target, query_file_name),
            dag=dag,
            snowflake_conn_id=snowflake_conn_id
        )

        run_logging_task = create_logging_gate(dag=dag,
                                                target=table_target,
                                                task_type="Transform Bronze into Silver")
        
        transform_task >> run_logging_task