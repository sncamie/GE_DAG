from airflow import AirflowException
import pandas as pd
import great_expectations as ge
import boto
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


c= boto.connect_s3()

bucket= c.lookup("omni-upload-mahle")
bucket_files = bucket.list('production_rsm9_bp/')
l=[(k.last_modified,k) for k in bucket_files]
key_to_download = sorted(l, cmp=lambda x,y: cmp(x[0], y[0]))[-1][1]

key_to_download.get_contents_to_filename('latestfile.parquet')

df=pd.read_parquet('latestfile.parquet')

def validate_data(expectation_suite, **kwargs):

    # Retrieve data context
    context = ge.data_context.DataContext("/home/dp-intern/Documents/GE_Dag/great_expectations/great_expectations.yml")

    # Create  batch_kwargs
    batch_request_1 = RuntimeBatchRequest(
        datasource_name="RSM9",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="batch1",   
        runtime_parameters={"batch_data": df},  
        batch_identifiers={"default_identifier_name": "latest batch"},
    )

    # Create batch (batch_kwargs + expectation suite)
    batch_file = context.get_batch(batch_request_1, expectation_suite)


    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch_file],
        run_id=f"airflow: {kwargs['dag_run'].run_id}:{kwargs['dag_run'].start_date}")

    # Handle result of validation
    if not results["success"]:
        raise AirflowException("Validation of the data is not successful")