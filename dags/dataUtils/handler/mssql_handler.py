import os
import logging
import csv
import pendulum
import datetime

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from dataUtils.basic.config_file import CSV_PATH_FILE, MSSQL_CONN_ID
from dataUtils.basic.table_utils import TableUtils
from dataUtils.handler.sql_file_handler import SqlFileHandler

from io import BytesIO

class MSSQLHandler:
    """
    Handles operations related to Microsoft SQL Server.
    """
    def __init__(self, sql_file_handler: SqlFileHandler, mssql_conn_id: str=MSSQL_CONN_ID, csv_path_file=CSV_PATH_FILE):
        self.mssql_conn_id = mssql_conn_id
        self.csv_path_file = csv_path_file
        self.sql_file_handler = sql_file_handler

    def extract_data(self, table_source: str, delimiter: str, query_file_name=None, file_name=None):
        """
        Extracts data from Microsoft SQL Server and saves it to a CSV file.

        Args:
            table_source (str): The source table.
            query_file_name (str): The name of the SQL file.

        Raises:
            AirflowException: If the CSV file cannot be created.
        """
        
        if file_name is None:
            file_name = TableUtils.premade_table_csv_name(table_source)
        
        data_buffer = BytesIO()
        
        sql_query = self.sql_file_handler.read_sql_into_string(table_source, query_file_name)
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        mssql_data_frame = mssql_hook.get_pandas_df(sql=sql_query)
        mssql_data_frame.to_csv(data_buffer, index=False, sep=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)

        return data_buffer
    
    def extract_fact_data(self, table_source, delimiter,
                          last_execution_date, query_file_name=None, file_name=None):
        """
        Extracts Fact data from Microsoft SQL Server and saves it to a CSV file.

        Args:
            table_source (str): The source table.
            query_file_name (str): The name of the SQL file.
            last_execution_date (str): The last execution date.

        Raises:
            AirflowException: If the CSV file cannot be created.
        """
        
        table_name = TableUtils.check_table_name(table_source)
        
        if file_name is None:
            file_name = TableUtils.premade_table_csv_name(table_name)

        full_file_path = os.path.join(self.csv_path_file, file_name)
        
        try:
            last_execution_date = eval(last_execution_date)[0]['LAST_EXECUTION_DATE'].strftime('%Y-%m-%d')
        except Exception as e:
            last_execution_date = '1990-01-01'

        data_buffer = BytesIO()
        
        sql_query = self.sql_file_handler.read_sql_fact_into_string(table_source, query_file_name, last_execution_date)
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        mssql_data_frame = mssql_hook.get_pandas_df(sql=sql_query)
        mssql_data_frame.to_csv(data_buffer, index=False, sep=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL)

        return data_buffer
        
        # if not mssql_data_frame.empty:
        #     mssql_data_frame.to_csv(full_file_path, index=False, sep= delimiter)
        #     logging.info(f"Finish exporting Fact {table_name.lower()}.csv to folder")
        # else:
        #     logging.info(f"Skip file since file importing is empty.")
        
       
        