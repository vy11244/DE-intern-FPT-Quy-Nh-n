from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from dataUtils.basic.task_creation import create_logging_task

class PipelineMaker():   
    def create_master_pipeline(self, dag, task_all: list, module_name: str):
        list_length = len(task_all)
        task_cache = None
        
        for count, task_list in enumerate(task_all): 
            
            temp_list = [TriggerDagRunOperator(
                task_id = f"trigger_{task_id}",
                trigger_dag_id=task_id,
                wait_for_completion=True,
                deferrable=True,
                dag=dag
            ) for task_id in task_list]
            
            if task_cache is not None:
                task_cache >> temp_list
                
            if not count >= list_length-1:
                dummy_task = DummyOperator(
                    task_id = f'checkpoint_{count}'
                )

                task_cache = dummy_task
                temp_list >> dummy_task
        