import os

from airflow.exceptions import AirflowException
from dataUtils.basic.config_file import SQL_PATH_FILE

class SqlFileHandler:
    """
    Handles reading SQL files and generating SQL strings for further use.
    """
    def __init__(self, sql_path_file: str=SQL_PATH_FILE):
        self.sql_path_file = os.path.abspath(sql_path_file)

    def read_sql_into_string(self, table_name: str, query_file_name: str):
        """
        Reads a SQL file and returns the SQL string with table_name parameter replaced.

        Args:
            table_name (str): The table name to substitute into the SQL.
            query_file_name (str): The name of the SQL file.

        Returns:
            str: SQL string.

        Raises:
            AirflowException: If the SQL file is not found.
        """
        full_query_path = os.path.join(os.path.abspath(self.sql_path_file), query_file_name)
        
        if not os.path.exists(full_query_path):
            raise AirflowException(f"No query file found in {full_query_path}.")

        with open(full_query_path, "r") as file:
            sql_str = file.read()

        return sql_str.format(table_name = table_name)
    
    def read_sql_fact_into_string(self, table_name: str, query_file_name:str, last_execution_date:str):
        """
        Reads a SQL file and returns the SQL string with table_name and last_execution_date parameter replaced.

        Args:
            table_name (str): The table name to substitute into the SQL.
            query_file_name (str): The name of the SQL file.
            last_execution_date (str): The last execution date.

        Returns:
            str: SQL string.

        Raises:
            AirflowException: If the SQL file is not found.
        """
        
        full_query_path = os.path.join(os.path.abspath(self.sql_path_file), query_file_name)
        print('Sql_path_file: ', self.sql_path_file)
        print('Query_file_name: ', query_file_name)

        if not os.path.exists(full_query_path):
            raise AirflowException(f"No query file found in {full_query_path}.")

        with open(full_query_path, "r") as file:
            sql_str = file.read()

        return sql_str.format(table_name= table_name,
                              last_execution_date=last_execution_date)