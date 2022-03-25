from utils import BatchRequest

# GE required names to set up datasources
# Fill in information for the fields specified
datasource_name = "random2"
expectation_suite_name = f"{datasource_name }_suite"
checkpoint_name = f"{datasource_name}_checkpoint"

# Data Asset fields
base_directory = "/home/ncamiso.khanyile/Data/"
group_names = ["data_asset_name"]
regex_pattern = "(.*)\.csv"
data_asset_name = "prescribe"
reader_method = "read_csv"

datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "module_name": "great_expectations.datasource",
    "execution_engine": {
        "module_name": "great_expectations.execution_engine",
        "class_name": "PandasExecutionEngine",
    },
    "data_connectors": {
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetFilesystemDataConnector",
            "base_directory": base_directory,
            "assets": {
                data_asset_name: {
                    "base_directory": "prescribe_prescribereportdata/",
                    "pattern": "(.*)\.csv",
                    "group_names": group_names,
                },
            },
        },
    },
}

batch_request = BatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_configured_data_connector_name",
    data_asset_name=data_asset_name,
    data_connector_query={"index": -1},
    batch_spec_passthrough={
        "reader_method": reader_method,
    },
)

# conifgure checkpoint

checkpoint_config = f"""
name: {checkpoint_name}
config_version: 1
class_name: Checkpoint
validations:
- batch_request: {batch_request}
  expectation_suite_name: {expectation_suite_name}
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
      site_names: []
"""
