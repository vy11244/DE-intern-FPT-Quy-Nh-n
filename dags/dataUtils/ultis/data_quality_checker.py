from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.dummy import DummyOperator

from dataUtils.basic.config_file import SNOWFLAKE_CONN_ID, LOGGING_TABLE_NAME

class DataQualityChecker:
    def __init__(self, database_name: str, schema_name: str):
        self.database_name = database_name
        self.schema_name = schema_name
        
    def initialize_rules(self, dag):
        query = """
USE DATABASE METADATA;

CREATE OR REPLACE FUNCTION validate_date("dateStr" STRING, format STRING)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    // Regular expression pattern for YYYY-MM-DD format
    var pattern = /^\d{4}-\d{2}-\d{2}$/;

    // Validate the date string format
    if (!pattern.test(dateStr)) {
        return false; // Date format is invalid
    }

    // Extract year, month, and day from the date string
    var parts = dateStr.split('-');
    var year = parseInt(parts[0]);
    var month = parseInt(parts[1]);
    var day = parseInt(parts[2]);

    // Check if month is in valid range (1-12)
    if (month < 1 || month > 12) {
        return false; // Month is out of range
    }

    // Check if day is in valid range based on the month
    var maxDay = new Date(year, month, 0).getDate(); // Get the maximum number of days for the given month
    if (day < 1 || day > maxDay) {
        return false; // Day is out of range
    }

    // Check if the whole date string is 10 characters in length
    if (dateStr.length !== 10) {
        return false; // Date string length is invalid
    }

    return true; // Date is valid
$$;

CREATE OR REPLACE FUNCTION validate_null("value" STRING)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!value) {
        return false
    }
    return true
$$;

CREATE OR REPLACE FUNCTION validate_length("value" STRING, "lenMin" float, "lenMax" float)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (value.length >= lenMax && value.length <= lenMin) {
        return true
    }
    return false
$$;

CREATE OR REPLACE FUNCTION validate_regex("value" STRING, "pattern" STRING)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    const regex = new RegExp(pattern);
    return regex.test(value);
$$;

CREATE OR REPLACE FUNCTION validate_data_type("value" STRING, "type" STRING)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    return typeof value === type;
$$;

CREATE OR REPLACE FUNCTION validate_value_list("value" STRING, "valueList" array)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    return valueList.includes(value)
$$;

CREATE OR REPLACE PROCEDURE RULE_DATE (
    DB_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, 
    CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var error_msg = [];
    var qry = `
    insert into METADATA.INTEGRATION.DQ_RULE_VALIDATION_RESULT (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT CONCAT('${DB_NAME}','.','${SCHEMA_NAME}','.','${TABLE_NAME}'),
        '${CONFIG_NAME["COL"]}',
        ${CONFIG_NAME["COL"]},
        concat('RULE_DATE: ','${CONFIG_NAME["FORMAT"]}'),
        '${CONFIG_NAME["COL"]} HAS INVALID DATE' AS ERR_MSG,
        object_construct(*) AS ERR_REC
    FROM "${DB_NAME}"."${SCHEMA_NAME}"."${TABLE_NAME}"
    WHERE validate_date(${CONFIG_NAME["COL"]},'${CONFIG_NAME["FORMAT"]}') = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
    
    try {
    var rs = snowflake.execute({ sqlText: qry });
    if(REQD_CLEANSE_RECORD)
    {
    var qry = `
    create temporary table if not exists ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    like ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME};`
    
    var rs_temp = snowflake.execute({ sqlText: qry });
    
    var qry = `
    insert into ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    select * from ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}
    WHERE validate_date(${CONFIG_NAME["COL"]},'${CONFIG_NAME["FORMAT"]}') = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL;`
    
    var rs_ins = snowflake.execute({ sqlText: qry });
    
    }
    return "RULE_DATE VALIDATION COMPLETED Successfully";
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
        error_code : ‘${err.code}’,
        error_state : ‘${err.state}’,
        error_message : ‘${err.message}’,
        stack_trace : ‘${err.stackTraceTxt}’
        } `);
    return error_msg;		  
    }
$$;

CREATE OR REPLACE PROCEDURE RULE_NULL (
    DB_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, 
    CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var error_msg = [];
    var qry = `
    insert into METADATA.INTEGRATION.DQ_RULE_VALIDATION_RESULT (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT CONCAT('${DB_NAME}','.','${SCHEMA_NAME}','.','${TABLE_NAME}'),
        '${CONFIG_NAME["COL"]}',
        ${CONFIG_NAME["COL"]},
        concat('RULE_NULL: ','CHECK NULL'),
        '${CONFIG_NAME["COL"]} IS NULL' AS ERR_MSG,
        object_construct(*) AS ERR_REC
    FROM "${DB_NAME}"."${SCHEMA_NAME}"."${TABLE_NAME}"
    WHERE validate_null(${CONFIG_NAME["COL"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
    
    try {
    var rs = snowflake.execute({ sqlText: qry });
    if(REQD_CLEANSE_RECORD)
    {
    var qry = `
    create temporary table if not exists ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    like ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME};`
    
    var rs_temp = snowflake.execute({ sqlText: qry });
    
    var qry = `
    insert into ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    select * from ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}
    WHERE validate_null(${CONFIG_NAME["COL"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL;`
    
    var rs_ins = snowflake.execute({ sqlText: qry });
    
    }
    return "RULE_NULL VALIDATION COMPLETED Successfully";
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
        error_code : ‘${err.code}’,
        error_state : ‘${err.state}’,
        error_message : ‘${err.message}’,
        stack_trace : ‘${err.stackTraceTxt}’
        } `);
    return error_msg;		  
    }
$$;

CREATE OR REPLACE PROCEDURE RULE_LENGTH (
    DB_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, 
    CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var error_msg = [];
    var qry = `
    insert into METADATA.INTEGRATION.DQ_RULE_VALIDATION_RESULT (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT CONCAT('${DB_NAME}','.','${SCHEMA_NAME}','.','${TABLE_NAME}'),
        '${CONFIG_NAME["COL"]}',
        ${CONFIG_NAME["COL"]},
        concat('RULE_LENGTH: MIN_LEN:', '${CONFIG_NAME["MIN_LEN"]}', '  MAX_LEN: ', '${CONFIG_NAME["MAX_LEN"]}'),
        '${CONFIG_NAME["COL"]} HAS INVALID LENGTH' AS ERR_MSG,
        object_construct(*) AS ERR_REC
    FROM "${DB_NAME}"."${SCHEMA_NAME}"."${TABLE_NAME}"
    WHERE validate_length(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["MIN_LEN"]}, ${CONFIG_NAME["MAX_LEN"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
    
    try {
    var rs = snowflake.execute({ sqlText: qry });
    if(REQD_CLEANSE_RECORD)
    {
    var qry = `
    create temporary table if not exists ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    like ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME};`
    
    var rs_temp = snowflake.execute({ sqlText: qry });
    
    var qry = `
    insert into ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    select * from ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}
    WHERE validate_length(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["MIN_LEN"]}, ${CONFIG_NAME["MAX_LEN"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL;`
    
    var rs_ins = snowflake.execute({ sqlText: qry });
    
    }
    return "RULE_LENGTH VALIDATION COMPLETED Successfully";
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
        error_code : ‘${err.code}’,
        error_state : ‘${err.state}’,
        error_message : ‘${err.message}’,
        stack_trace : ‘${err.stackTraceTxt}’
        } `);
    return error_msg;		  
    }
$$;

CREATE OR REPLACE PROCEDURE RULE_REGEX (
    DB_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, 
    CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var error_msg = [];
    var qry = `
    insert into METADATA.INTEGRATION.DQ_RULE_VALIDATION_RESULT (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT CONCAT('${DB_NAME}','.','${SCHEMA_NAME}','.','${TABLE_NAME}'),
        '${CONFIG_NAME["COL"]}',
        ${CONFIG_NAME["COL"]},
        concat('RULE_LENGTH: PATTERN:', '${CONFIG_NAME["PATTERN"]}),
        '${CONFIG_NAME["COL"]} DOES NOT MATCH REGEX PATTERN' AS ERR_MSG,
        object_construct(*) AS ERR_REC
    FROM "${DB_NAME}"."${SCHEMA_NAME}"."${TABLE_NAME}"
    WHERE validate_regex(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["PATTERN"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
    
    try {
    var rs = snowflake.execute({ sqlText: qry });
    if(REQD_CLEANSE_RECORD)
    {
    var qry = `
    create temporary table if not exists ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    like ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME};`
    
    var rs_temp = snowflake.execute({ sqlText: qry });
    
    var qry = `
    insert into ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    select * from ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}
    WHERE validate_regex(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["PATTERN"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL;`
    
    var rs_ins = snowflake.execute({ sqlText: qry });
    
    }
    return "RULE_REGEX VALIDATION COMPLETED Successfully";
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
        error_code : ‘${err.code}’,
        error_state : ‘${err.state}’,
        error_message : ‘${err.message}’,
        stack_trace : ‘${err.stackTraceTxt}’
        } `);
    return error_msg;		  
    }
$$;

CREATE OR REPLACE PROCEDURE RULE_DATA_TYPE (
    DB_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, 
    CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var error_msg = [];
    var qry = `
    insert into METADATA.INTEGRATION.DQ_RULE_VALIDATION_RESULT (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT CONCAT('${DB_NAME}','.','${SCHEMA_NAME}','.','${TABLE_NAME}'),
        '${CONFIG_NAME["COL"]}',
        ${CONFIG_NAME["COL"]},
        concat('RULE_LENGTH: DATA TYPE:', '${CONFIG_NAME["DATA_TYPE"]}),
        '${CONFIG_NAME["COL"]} DOES NOT MATCH DATA TYPE' AS ERR_MSG,
        object_construct(*) AS ERR_REC
    FROM "${DB_NAME}"."${SCHEMA_NAME}"."${TABLE_NAME}"
    WHERE validate_data_type(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["DATA_TYPE"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
    
    try {
    var rs = snowflake.execute({ sqlText: qry });
    if(REQD_CLEANSE_RECORD)
    {
    var qry = `
    create temporary table if not exists ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    like ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME};`
    
    var rs_temp = snowflake.execute({ sqlText: qry });
    
    var qry = `
    insert into ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    select * from ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}
    WHERE validate_data_type(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["DATA_TYPE"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL;`
    
    var rs_ins = snowflake.execute({ sqlText: qry });
    
    }
    return "RULE_DATA_TYPE COMPLETED Successfully";
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
        error_code : ‘${err.code}’,
        error_state : ‘${err.state}’,
        error_message : ‘${err.message}’,
        stack_trace : ‘${err.stackTraceTxt}’
        } `);
    return error_msg;		  
    }
$$;

CREATE OR REPLACE PROCEDURE RULE_VALUE_LIST  (
    DB_NAME VARCHAR, SCHEMA_NAME VARCHAR, TABLE_NAME VARCHAR, 
    CONFIG_NAME VARIANT, REQD_CLEANSE_RECORD BOOLEAN)
RETURNS VARIANT NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var error_msg = [];
    
    var qry = `
    insert into METADATA.INTEGRATION.DQ_RULE_VALIDATION_RESULT (table_name,col_name,invalid_value,DQ_RULE,err_msg,err_rec)
    SELECT CONCAT('${DB_NAME}','.','${SCHEMA_NAME}','.','${TABLE_NAME}'),
        '${CONFIG_NAME["COL"]}',
        ${CONFIG_NAME["COL"]},
        concat('RULE_LENGTH: LIST:', '${CONFIG_NAME["VALUE_LIST"]}),
        '${CONFIG_NAME["COL"]} IS NOT IN THE LIST' AS ERR_MSG,
        object_construct(*) AS ERR_REC
    FROM "${DB_NAME}"."${SCHEMA_NAME}"."${TABLE_NAME}"
    WHERE validate_data_type(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["VALUE_LIST"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL
    ;`
    
    try {
    var rs = snowflake.execute({ sqlText: qry });
    if(REQD_CLEANSE_RECORD)
    {
    var qry = `
    create temporary table if not exists ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    like ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME};`
    
    var rs_temp = snowflake.execute({ sqlText: qry });
    
    var qry = `
    insert into ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}_TEMP
    select * from ${DB_NAME}.${SCHEMA_NAME}.${TABLE_NAME}
    WHERE validate_data_type(${CONFIG_NAME["COL"]}, ${CONFIG_NAME["VALUE_LIST"]}) = false
    and ${CONFIG_NAME["COL"]} IS NOT NULL;`
    
    var rs_ins = snowflake.execute({ sqlText: qry });
    
    }
    return "RULE_VALUE_LIST COMPLETED Successfully";
    } catch (err) {
        error_msg.push(` {
        sql statement : ‘${qry}’,
        error_code : ‘${err.code}’,
        error_state : ‘${err.state}’,
        error_message : ‘${err.message}’,
        stack_trace : ‘${err.stackTraceTxt}’
        } `);
    return error_msg;		  
    }
$$;
        """
        
        return SnowflakeOperator(
            task_id="intialize_rules",
            dag=dag,
            snowflake_conn_id = SNOWFLAKE_CONN_ID,
            sql=query
        )
        
        
    def create_rule_date_task(self, dag, table_name: str, col_name: str, date_format: str, append_record: str = 'true'):
        query = f"BEGIN TRANSACTION; USE DATABASE METADATA; USE SCHEMA INTEGRATION; call RULE_DATE('{self.database_name}', '{self.schema_name}', '{table_name}', PARSE_JSON('" + "{" + f'"COL":"{col_name}", "FORMAT": "{date_format}"' + '}' + f"'), {append_record}); COMMIT;"
                
        return SnowflakeOperator(
            task_id=f"validate_date_{self.database_name}.{self.schema_name}.{table_name}",
            dag=dag,
            sql=query,
            snowflake_conn_id = SNOWFLAKE_CONN_ID
        )
        
    def create_rule_length(self, dag, table_name: str, col_name: str, min_len: int, max_len: int, append_record: str = 'true'):
        query = f"BEGIN TRANSACTION; USE DATABASE METADATA; USE SCHEMA INTEGRATION; call RULE_LENGTH('{self.database_name}', '{self.schema_name}', '{table_name}', PARSE_JSON('" + "{" + f'"COL":"{col_name}", "MIN_LEN": "{min_len}", "MAX_LEN": "{max_len}"' + '}' + f"'), {append_record}); COMMIT;"
                
        return SnowflakeOperator(
            task_id=f"validate_length_{self.database_name}.{self.schema_name}.{table_name}",
            dag=dag,
            sql=query,
            snowflake_conn_id = SNOWFLAKE_CONN_ID
        )
        
    def create_rule_regex(self, dag, table_name: str, col_name: str, pattern: str, append_record: str = 'true'):
        query = f"BEGIN TRANSACTION; USE DATABASE METADATA; USE SCHEMA INTEGRATION; call RULE_REGEX('{self.database_name}', '{self.schema_name}', '{table_name}', PARSE_JSON('" + "{" + f'"COL":"{col_name}", "PATTERN": "{pattern}"' + '}' + f"'), {append_record}); COMMIT;"
                
        return SnowflakeOperator(
            task_id=f"validate_regex_{self.database_name}.{self.schema_name}.{table_name}",
            dag=dag,
            sql=query,
            snowflake_conn_id = SNOWFLAKE_CONN_ID
        )
        
    def create_rule_null(self, dag, table_name: str, col_name: str, append_record: str = 'true'):
        query = f"BEGIN TRANSACTION; USE DATABASE METADATA; USE SCHEMA INTEGRATION; call RULE_NULL('{self.database_name}', '{self.schema_name}', '{table_name}', PARSE_JSON('" + "{" + f'"COL":"{col_name}"' + '}' + f"'), {append_record}); COMMIT;"
                
        return SnowflakeOperator(
            task_id=f"validate_null_{self.database_name}.{self.schema_name}.{table_name}",
            dag=dag,
            sql=query,
            snowflake_conn_id = SNOWFLAKE_CONN_ID
        )
        
    def create_rule_value_list(self, dag, table_name: str, col_name: str, value_list: list, append_record: str = 'true'):
        query = f"BEGIN TRANSACTION; USE DATABASE METADATA; USE SCHEMA INTEGRATION; call RULE_VALUE_LIST('{self.database_name}', '{self.schema_name}', '{table_name}', PARSE_JSON('" + "{" + f'"COL":"{col_name}", "VALUE_LIST":"{value_list}"' + '}' + f"'), {append_record}); COMMIT;"
                
        return SnowflakeOperator(
            task_id=f"validate_value_list_{self.database_name}.{self.schema_name}.{table_name}",
            dag=dag,
            sql=query,
            snowflake_conn_id = SNOWFLAKE_CONN_ID
        )
        