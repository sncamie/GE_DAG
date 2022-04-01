from great_expectations.checkpoint.checkpoint import SimpleCheckpoint
from great_expectations.core.batch import BatchRequest
import great_expectations as ge
from great_expectations.data_context.data_context import DataContext
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)

#context = DataContext(context_root_dir="/home/ncamiso.khanyile/great_expectations")


context = ge.data_context.DataContext()

expectation_suite_name= "my_suite_name"

batch_request = {
    "datasource_name": "RSM9",
    "data_connector_name": "default_configured_data_connector_name",
    "data_asset_name": "RSM9",
    "limit": 1000,
}


validator = context.get_validator(
    batch_request=BatchRequest(**batch_request),
    expectation_suite_name=expectation_suite_name,
)

excluded_expectations = [
    "expect_column_quantile_values_to_be_between",
    "expect_table_row_count_to_equal",
    "expect_column_unique_value_count_to_be_between",
    "expect_column_values_to_be_in_set",
    "expect_column_values_to_be_of_type",
]
ignored_columns = [
    "Unnamed: 0",
    # "mainplc/CalibrationDriveSpeed",
    # "mainplc/CalibrationDriveSpeedPercent",
    "mainplc/FormingDriveSpeed",
    # "mainplc/FormingDriveSpeedPercent",
    # "mainplc/SpeedDifferencePercent",
    "bsp/CalibrationCurrent_Scaled",
    # "bsp/CalibrationDriveSpeedPercent_Scaled",
    "bsp/CalibrationSpeed_Scaled",
    # "mainplc/WeldCurrent_Scaled",
]


profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=excluded_expectations,
    ignored_columns=ignored_columns,
    not_null_only=not_null_only,
    table_expectations_only=table_expectations_only,
    value_set_threshold=value_set_threshold,
)


suite = profiler.build_suite()


# Review and save our Expectation Suite
print(validator.get_expectation_suite(discard_failed_expectations=False))
validator.save_expectation_suite(discard_failed_expectations=False)

# Set up and run a Simple Checkpoint for ad hoc validation of our data
checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
        }
    ],
}
checkpoint = SimpleCheckpoint(
    f"_tmp_checkpoint_{expectation_suite_name}", context, **checkpoint_config
)
checkpoint_result = checkpoint.run()

# Build Data Docs
context.build_data_docs()

# Get the only validation_result_identifier from our SimpleCheckpoint run, and open Data Docs to that page
validation_result_identifier = checkpoint_result.list_validation_result_identifiers()[0]
context.open_data_docs(resource_identifier=validation_result_identifier)
