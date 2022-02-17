from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from validation import validate_checkpoint
from load_omni_data import load_omni_raw, load_bobs, load_prescribe_data, root_directory
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


dag = DAG(
    dag_id="validate_mahle_data",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2022, 2, 10),
    max_active_runs=1,
    catchup=False,
)

validate_s3_raw_data = PythonOperator(
    dag=dag,
    task_id="validate_s3_raw_data",
    python_callable=validate_checkpoint,
    op_kwargs={"checkpoint_name": "RSM9_checkpoint"},
)

load_raw_omni_data=PythonOperator(
    dag=dag,
    task_id ='load_raw_omni_data',
    python_callable = load_omni_raw,
    op_kwargs={"loc_path": f"{root_directory}/data_store_rawvalue/raw.csv"},
)

validate_omni_raw = PythonOperator(
     dag=dag,
    task_id="validate_omni_raw_data",
    python_callable=validate_checkpoint,
    op_kwargs={"checkpoint_name": "omni_raw_checkpoint"},
)


load_bobs=PythonOperator(
    dag=dag,
    task_id ='load_bobs',
    python_callable = load_bobs,
    op_kwargs={"loc_path": f"{root_directory}/bob_bobstats/bobs.csv"},
)


validate_bobs = PythonOperator(
     dag=dag,
    task_id="validate_bobs",
    python_callable=validate_checkpoint,
    op_kwargs={"checkpoint_name": "bobs_checkpoint"},
)

load_prescribe_data=PythonOperator(
    dag=dag,
    task_id ='load_prescribe_data',
    python_callable = load_prescribe_data,
    op_kwargs={"loc_path": f"{root_directory}/prescribe_prescribereportdata/prescribe.csv"},
    
)

validate_prescribe = PythonOperator(
     dag=dag,
    task_id="validate_prescription",
    python_callable=validate_checkpoint,
    op_kwargs={"checkpoint_name": "prescribe_checkpoint"},

)
end_of_data_pipeline = DummyOperator(dag=dag, task_id="end_of_data_pipeline")


validate_s3_raw_data>>[load_raw_omni_data,load_bobs,load_prescribe_data]>>validate_omni_raw>>validate_bobs>>validate_prescribe>>end_of_data_pipeline

