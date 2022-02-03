
import pandas as pd
from ruamel import yaml
import shutup

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest

context = ge.get_context()

shutup.please()



checkpoint_config = {
    "name": "my_checkpoint",
    "config_version": 1,
    "class_name": "SimpleCheckpoint",
    "expectation_suite_name": "RSM9_suite",
}
context.add_checkpoint(**checkpoint_config)


df_1 = pd.read_parquet("/home/dp-intern/Documents/MahleParquets/Data/2020-06-18 13:56:05--2020-06-18 14:11:05.parquet")
df_2 = pd.read_parquet("/home/dp-intern/Documents/MahleParquets/Data/2020-06-21 09:38:39--2020-06-21 09:53:40.parquet")

batch_request_1 = RuntimeBatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="batch1",   
    runtime_parameters={"batch_data": df_1},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "batch_2"},
)

batch_request_2 = RuntimeBatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="batch_2",  # This can be anything that identifies this data_asset for you
    runtime_parameters={"batch_data": df_2},  # Pass your DataFrame here.
    batch_identifiers={"default_identifier_name": "batch_1"},
)

results = context.run_checkpoint(
    checkpoint_name="my_checkpoint",
    validations=[
        {"batch_request": batch_request_1},
        {"batch_request": batch_request_2},
    ],
)






context.build_data_docs()

batch_1_result = results.list_validation_result_identifiers()[0]
batch_2_result = results.list_validation_result_identifiers()[1]
context.open_data_docs(resource_identifier=batch_1_result)
context.open_data_docs(resource_identifier=batch_2_result)