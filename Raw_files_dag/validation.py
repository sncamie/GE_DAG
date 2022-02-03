from airflow import AirflowException
from airflow.operators.python_operator import PythonOperator
import great_expectations as ge

...

def validate_data(ds, **kwargs):

    # Retrieve your data context
    context = ge.data_context.DataContext("path to great_expectations.yml")

    # Create your batch_kwargs
    batch_kwargs_file = {
        "path": "path to data file",
        "datasource": "my_pandas_datasource"}

    # Create your batch (batch_kwargs + expectation suite)
    batch_file = context.get_batch(batch_kwargs_file, "expectation suite")


    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch_file],
        # This run_id can be whatever you choose
        run_id=f"airflow: {kwargs['dag_run'].run_id}:{kwargs['dag_run'].start_date}")

    # Handle result of validation
    if not results["success"]:
        raise AirflowException("Validation of the data is not successful")