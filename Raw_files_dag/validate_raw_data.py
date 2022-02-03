from datetime import date, datetime, timedelta
from airflow import AirflowException
from airflow.operators.python import PythonOperator
import great_expectations as ge
from airflow.operators.dummy import DummyOperator
from validation import *
from airflow import DAG
import shutup

shutup.please()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'Raw data validation',
    default_args=default_args,
    description='validate raw data DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,   
) as dag:





    task_validate_data = PythonOperator(
        task_id='task_validate_data',
        python_callable=validate_data,
        provide_context=True,
        op_kwargs={"checkpoint_name": "RSM9_checkpoint"},
        dag=dag
        )

    end_of_data_pipeline = DummyOperator(dag=dag, task_id="end_of_data_pipeline")
    

    task_validate_data>>end_of_data_pipeline