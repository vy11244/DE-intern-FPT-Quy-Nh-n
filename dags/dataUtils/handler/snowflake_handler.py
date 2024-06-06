from airflow.exceptions import AirflowException
from dataUtils.basic.config_file import SNOWFLAKE_CONN_ID, DEFAULT_DELIMITER
from dataUtils.handler.blob_storage_handler import BlobStorageHandler
from dataUtils.basic.table_utils import TableUtils
from dataUtils.handler.json_file_handler import JsonFileHandler

import datetime


class SnowflakeHandler:
    """
    Handles operations related to Snowflake.
    """

    def __init__(
        self,
        blob_storage_handler: BlobStorageHandler,
        json_file_handler: JsonFileHandler,
        snowflake_conn_id: str = SNOWFLAKE_CONN_ID,
    ):
        self.blob_handler = blob_storage_handler
        self.snowflake_conn_id = snowflake_conn_id
        self.json_file_handler = json_file_handler

    def create_extract_query(
        self,
        table_lake: str,
        table_target: str,
        module_name: str,
        target_primary_key: str,
        method: str,
        json_file_name: str,
        delimiter: str = DEFAULT_DELIMITER,
        latest_date: str = None,
    ):

        """
        Generates an extract query for Snowflake database based on provided parameters.

        Parameters:
        - table_lake (str): Name of the source table or dataset.
        - table_target (str): Name of the target table in Snowflake.
        - module_name (str): Name of the module or project.
        - primary_key (str): Name of the primary key column.
        - method (str): Method of data extraction. Options: "insert", "upsert", or "replace".
        - json_file_name (str): Name of the JSON file containing column mapping information.
        - blob_file_path (str, optional): Path to the blob file. Defaults to None.

        Raises:
        - AirflowException: If the method is not one of "insert", "upsert", or "replace".

        Returns:
        - str: SQL query for Snowflake data extraction.
        """

        # Check if the method is valid
        if method not in ["insert", "upsert", "replace"]:
            raise AirflowException(
                f"There is no method for {method}, method must be insert, upsert or replace"
            )

        # Sanitize table names
        table_lake = TableUtils.check_table_name(table_lake)
        table_target = TableUtils.check_table_name(table_target)

        blob_packet = self.blob_handler.get_blob_service_account_credential()
        # blob_name = self.blob_handler.get_latest_blob(blob_packet)
        try:
            latest_date = eval(latest_date)[0]["LAST_EXECUTION_DATE"].strftime(
                "%Y_%m_%d"
            )
            blob_name = (
                f"landing/{table_lake.lower()}/{table_lake.lower()}_{latest_date}.csv"
            )
        except Exception as e:
            blob_path = TableUtils.premade_blob_file_path(table_lake)
            blob_name = self.blob_handler.get_latest_blob(blob_packet, blob_path)

        # Read column mapping from JSON file
        column_mapping = self.json_file_handler.read_json_into_mapping_str(
            json_file_name
        )

        # Construct first part of the SQL query
        first_query_part = f"""
EXECUTE IMMEDIATE $$

DECLARE 
    record_count INTEGER;

BEGIN
    BEGIN TRANSACTION;
    USE DATABASE {module_name};
    USE SCHEMA {module_name}_BRONZE;

    CREATE OR REPLACE TEMP STAGE {table_target}_{module_name}_stage
        URL= 'azure://{blob_packet.account_url.replace("https://", '')}{blob_packet.container_name}/{blob_name}'
        CREDENTIALS=(AZURE_SAS_TOKEN='{blob_packet.credential}')
        FILE_FORMAT = (TYPE=CSV COMPRESSION=NONE SKIP_HEADER=0 SKIP_BLANK_LINES=TRUE FIELD_DELIMITER='{delimiter}' FIELD_OPTIONALLY_ENCLOSED_BY = '"');

    CREATE OR REPLACE TEMP FILE FORMAT {table_target}_{module_name}_csv_ffs
        TYPE = CSV
        PARSE_HEADER = TRUE
        FIELD_DELIMITER='{delimiter}'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"';

    SELECT COUNT(*) INTO record_count FROM '@{table_target}_{module_name}_stage';

    IF (record_count > 1) THEN
        CREATE OR REPLACE TEMP TABLE {table_target}_{module_name}_temp_table
        USING TEMPLATE
        (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
        FROM TABLE(
            INFER_SCHEMA(
            LOCATION=>'@{table_target}_{module_name}_stage'
            , FILE_FORMAT=>'{table_target}_{module_name}_csv_ffs'
            )
            ));

        COPY INTO {table_target}_{module_name}_temp_table
        FROM @{table_target}_{module_name}_stage
        FILE_FORMAT = (TYPE=CSV SKIP_HEADER=1 FIELD_DELIMITER='{delimiter}' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
"""

        # Construct second part of the SQL query based on the method
        second_query_part = None

        if method == "replace":
            if target_primary_key is not None or "" or " ":
                optional_primary_key = (
                    f"ALTER TABLE {table_target} ADD PRIMARY KEY ({target_primary_key})"
                )

            second_query_part = f"""
        TRUNCATE TABLE {table_target};
        INSERT INTO {table_target}
        SELECT {column_mapping}
        FROM {table_target}_{module_name}_temp_table;
    """

        if method == "upsert":
            try:
                # if target_primary_key is None or "" or " ":
                #     raise AirflowException(f"Must contain a primary key to use upsert method.")

                # Extract column names from column_mapping
                table_target_name = [
                    chunk.split(" ")[2] for chunk in column_mapping.split(", ")
                ]
                # Construct SET and VALUES parts for the MERGE statement
                set_new_value = (", ").join(
                    [
                        f"{column_name} = table_source.{column_name}"
                        for column_name in (table_target_name)
                    ]
                )
                new_value = (", ").join(
                    [f"table_source.{column_name}" for column_name in table_target_name]
                )

                second_query_part = f"""
        MERGE INTO {table_target} AS TABLE_TARGET USING
        (
            SELECT {column_mapping}
            FROM {table_target}_{module_name}_temp_table
        ) table_source ON table_target."{target_primary_key}" = table_source."{target_primary_key}"
        WHEN MATCHED THEN
        UPDATE SET 
            {set_new_value}
        WHEN NOT MATCHED THEN
        INSERT
            ({(", ").join(table_target_name)}) VALUES
            ({new_value});
    """
            except Exception as e:
                raise AirflowException("An error occurred during upsert operation")

        if method == "insert":
            second_query_part = f"""
        INSERT INTO {table_target}
        SELECT {column_mapping}
        FROM {table_target}_{module_name}_temp_table;
    """

        # Check if second_query_part is initialized
        if second_query_part is None:
            raise AirflowException("Failed to assemble query for Snowflake.")

        third_query_part = """
        COMMIT;
    ELSE
        ROLLBACK;
    END IF;
END;
$$
;
    """
        # Return the assembled SQL query
        return first_query_part + second_query_part + third_query_part
