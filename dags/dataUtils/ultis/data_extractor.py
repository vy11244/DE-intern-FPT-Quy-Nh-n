from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.dummy import DummyOperator

from dataUtils.handler.mssql_handler import MSSQLHandler
from dataUtils.handler.blob_storage_handler import BlobStorageHandler
from dataUtils.basic.config_file import DEFAULT_DELIMITER
from dataUtils.basic.task_creation import (
    create_logging_gate,
    create_check_failed_task,
    create_get_latest_task_etl_table,
    create_rerun_task
)
from dataUtils.basic.table_utils import TableUtils


class DataExtractor:
    """
    Orchestrates the extraction of data from various sources.
    """

    def __init__(
        self,
        source: object,
        target: object,
    ):
        self.source = source
        self.target = target

    def create_mssql_dag_task_local(
        self,
        dag,
        table_source,
        query_file_name,
        delimiter: str = DEFAULT_DELIMITER,
        is_fact: bool = False,
        re_run: bool = False
    ):
        """
        Creates a DAG task for extracting data from Microsoft SQL Server.

        Args:
            dag (DAG): The parent DAG.
            table_source (str): The source table.
            query_file_name (str): The name of the SQL file.

        Returns:
            PythonOperator: DAG task.
        """
        if isinstance(self.source, MSSQLHandler) and isinstance(
            self.target, BlobStorageHandler
        ):
            self.target.get_blob_service_account_credential()
            blob_serivce_client = self.target.get_blob_service_client()[0]

            if not is_fact:
                upload_to_adls_task = PythonOperator(
                    task_id=f"upload_{table_source}_to_adls",
                    python_callable=self.target.upload_file_to_azure,
                    dag=dag,
                    op_kwargs={
                        "blob_service_client": blob_serivce_client,
                        "table_source": table_source,
                        "query_file_name": query_file_name,
                        "delimiter": delimiter,
                        "blob_file_path": None,
                        "mssql_handler": self.source,
                    },
                )
            else:
                if re_run:
                    rerun_task = create_rerun_task(dag=dag, source_name=table_source)
                
                get_latest_task = create_get_latest_task_etl_table(
                    dag=dag,
                    source_name=table_source,
                    task_type="Extract Source into Lake",
                )

                upload_to_adls_task = PythonOperator(
                    task_id=f"upload_{table_source}_to_adls",
                    python_callable=self.target.upload_file_to_azure,
                    dag=dag,
                    op_kwargs={
                        "blob_service_client": blob_serivce_client,
                        "table_source": table_source,
                        "query_file_name": query_file_name,
                        "delimiter": delimiter,
                        "blob_file_path": None,
                        "mssql_handler": self.source,
                        "fact_table": True,
                        "last_execution_date": "{{ task_instance.xcom_pull(task_ids='" + f'get_latest_{TableUtils.check_table_name(table_source)}' + "') }}",
                    },
                )

            run_logging_task = create_logging_gate(
                dag=dag,
                source=TableUtils.check_table_name(table_source),
                task_type="Extract Source into Lake",
            )

            run_check_failed_task = create_check_failed_task(dag=dag)

            if not is_fact:
                upload_to_adls_task >> run_logging_task >> run_check_failed_task
                return upload_to_adls_task
            else:
                (
                    get_latest_task
                    >> upload_to_adls_task
                    >> run_logging_task
                    >> run_check_failed_task
                )
                                
            if re_run:
                rerun_task >> get_latest_task
                return rerun_task
                
            return get_latest_task
        else:
            raise AirflowException("Source object is not suitable for MSSQL")

    def create_blob_dag_task_upload(self, dag, table_source, blob_file_path):
        """
        Creates a DAG task for uploading data to Azure Blob Storage.

        Args:
            dag (DAG): The parent DAG.
            table_source (str): The source table.
            blob_file_path (str): The Azure Blob Storage path.

        Returns:
            PythonOperator: DAG task.
        """

        return DummyOperator(dag=dag, task_id="dummy_task_to_avoid_error")
