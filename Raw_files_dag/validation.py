from airflow import AirflowException
import pandas as pd
import great_expectations as ge
import boto
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


def validate_data(expectation_suite, **kwargs):

    # Retrieve data context
    context = ge.data_context.DataContext("/home/ncamiso.khanyile/great_expectations/great_expectations.yml")

    # Create  batch_kwargs
    batch_request_3 = RuntimeBatchRequest(
    datasource_name= "RSM9",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="mahlefiles",  
    runtime_parameters={"path": "s3://omni-upload-mahle/production_rsm9_bsp/2020-05-07T10:19:27/dataframe.parquet"},  
    batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    # Create batch (batch_kwargs + expectation suite)
    batch_file = context.get_batch(batch_request_3, expectation_suite)


    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch_file],
        run_id=f"airflow: {kwargs['dag_run'].run_id}:{kwargs['dag_run'].start_date}")

    # Handle result of validation
    if not results["success"]:
        raise AirflowException("Validation of the data is not successful")