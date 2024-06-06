from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.db import provide_session
from airflow.models import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.state import State
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.filesystem import FileSensor

from dataUtils.basic.config_file import LOGGING_TABLE_NAME, SNOWFLAKE_CONN_ID, CSV_PATH_FILE
from dataUtils.basic.table_utils import TableUtils

import pendulum
import time
import os

def delay_function(delay: int):
    time.sleep(delay)

@provide_session
def cleanup_xcom(session=None, **context):
    dag = context["dag"]
    dag_id = dag._dag_id 
    # It will delete all xcom of the dag_id
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()

def create_cleanup_task(dag):
    return PythonOperator(
        task_id="clean_xcom",
        python_callable = cleanup_xcom,
        provide_context=True, 
        dag=dag,
        trigger_rule="all_done",
    )

def create_delay_task(dag, delay: int):
    return PythonOperator(
        task_id=f"delay_{delay}_second",
        python_callable=delay_function,
        op_kwargs={
            'delay': delay
        },
        provide_context=True,
        dag=dag
    )

def create_get_latest_task_etl_table(dag, source_name: str, task_type: str, etl_table_name: str = LOGGING_TABLE_NAME, account_for_success: bool = True):
    if account_for_success:
        account_query = "AND TASK_LOG='Success'"
    else:
        account_query = ''
        
    source_name = TableUtils.check_table_name(source_name)

    query = f"""
        SELECT LAST_EXECUTION_DATE FROM {etl_table_name}
        WHERE SOURCE='{source_name}' AND TASK_TYPE='{task_type}' {account_query}
        ORDER BY LAST_EXECUTION_DATE DESC NULLS LAST LIMIT 1;
    """
    
    return SnowflakeOperator(
        dag=dag,
        task_id=f"get_latest_{source_name}",
        sql=query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

def get_failed_tasks(session=None, execution_date=pendulum.now(), **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    
    dag_run = DagRun.find(dag_id=dag_id, state=State.RUNNING)
    session = Session()
    
    try:
        failed_task_instances = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag_id)
            .filter(TaskInstance.execution_date == dag_run[0].execution_date)
            .filter(TaskInstance.state == State.FAILED)
            .all()
        )
        
        failed_task_list = [task_instance.task_id for task_instance in failed_task_instances]
        if failed_task_list:
            return 'Failed task(s): ' + ', '.join(failed_task_list)
        else:
            return 'Success'
        
    except Exception as e:
        return 'get_failed_tasks was unable to run.'
    
    finally:
        session.close()

def check_failed_task(session=None, execution_date=pendulum.now(), **context):
    dag = context["dag"]
    dag_id = dag._dag_id
    
    dag_run = DagRun.find(dag_id=dag_id, state=State.RUNNING)
    session = Session()
    
    try:
        failed_task_instances = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag_id)
            .filter(TaskInstance.execution_date == dag_run[0].execution_date)
            .filter(TaskInstance.state == State.FAILED)
            .all()
        )
        
        failed_task_list = [task_instance.task_id for task_instance in failed_task_instances]
        
        if failed_task_list:
            raise Exception("Failed task(s) found. Set DAG as FAILED.")
    
    finally:
        session.close()

def create_logging_failed_tasks(dag):
    return PythonOperator(
        task_id="logging_failed_tasks",
        python_callable=get_failed_tasks,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )
    
def create_check_failed_task(dag):
    return PythonOperator(
        task_id="check_failed_task",
        python_callable=check_failed_task,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )

def create_logging_task(dag, source: str="", target: str="",
                        task_type: str="", logging_table_name: str=LOGGING_TABLE_NAME, last_execution_date=pendulum.now()):
    
    source = TableUtils.check_table_name(source)
    
    query = """
        CREATE TABLE IF NOT EXISTS {logging_table_name} (
            TASK_ID INTEGER AUTOINCREMENT START 1 INCREMENT 1,
            SOURCE VARCHAR(150),
            TARGET VARCHAR(150),
            TASK_TYPE VARCHAR(150),
            LAST_EXECUTION_DATE DATE,
            TASK_LOG VARCHAR(250)
        );
        
        INSERT INTO {logging_table_name} (SOURCE, TARGET, TASK_TYPE, LAST_EXECUTION_DATE, TASK_LOG)
        VALUES ('{source}', '{target}', '{task_type}', '{last_execution_date}', '{task_log}')
    """
        
    return SnowflakeOperator(
        task_id=f'logging_task_query_{source}_{target}',
        dag=dag,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=query.format(logging_table_name=logging_table_name,
                         source=source,
                         target=target,
                         task_type=task_type,
                         last_execution_date=last_execution_date,
                         task_log="{{ task_instance.xcom_pull(task_ids='" + 'logging_failed_tasks' + "') }}"),
        trigger_rule=TriggerRule.ALL_DONE
    )

def create_rerun_task(dag, source_name: str, etl_table_name: str = LOGGING_TABLE_NAME):
    source_name = TableUtils.check_table_name(source_name)

    query = f"""
        DELETE FROM {etl_table_name}
        WHERE SOURCE='{source_name}'
    """
    
    return SnowflakeOperator(
        dag=dag,
        task_id=f"rerun_{source_name}",
        sql=query,
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )
      
def create_logging_gate(dag, source: str="", target: str="", task_type: str=""):
    run_logging_failed_tasks = create_logging_failed_tasks(dag=dag)
    run_logging_task = create_logging_task(dag=dag, 
                                            source=source,
                                            target=target,
                                            task_type=task_type)
    
    run_logging_failed_tasks >> run_logging_task
    return run_logging_failed_tasks

def create_logging_gate_with_cleanup(dag, source: str="", target: str="", task_type: str=""):
    run_logging_failed_tasks = create_logging_failed_tasks(dag=dag)
    run_logging_task = create_logging_task(dag=dag, 
                                            source=source,
                                            target=target,
                                            task_type=task_type)
    run_cleanup_task = create_cleanup_task(dag=dag)
    
    run_logging_failed_tasks >> run_logging_task >> run_cleanup_task
    return run_logging_failed_tasks