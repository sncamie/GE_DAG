from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.rule_based_profiler import \
    RuleBasedProfiler
from ruamel import yaml
import time
import great_expectations as ge
from great_expectations import DataContext
from great_expectations.checkpoint import SimpleCheckpoint

context = ge.get_context()

start= time.time()

profiler_config = """
name: S3 raw profiler
config_version: 1.0

variables:
  false_positive_rate: 0.01
  mostly: 0.99
  s3_batch_request:
    datasource_name: RSM9
    data_connector_name: default_configured_data_connector_name
    data_asset_name: RSM9
    data_connector_query:
      index: "-6:-1"
  quantiles:
    - 2.5e-1
    - 5.0e-1
    - 7.5e-1
  allow_relative_error: linear
  num_bootstrap_samples: 9139
  bootstrap_random_seed: 43792


rules:
  column_count_rule:
    domain_builder:
        class_name: TableDomainBuilder
    parameter_builders:
      - name: column_count_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request: $variables.s3_batch_request
        metric_name: table.column_count
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 0
        truncate_values:
          lower_bound: 0
    expectation_configuration_builders:
      - expectation_type: expect_table_column_count_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        min_value: $parameter.column_count_range.value.value_range[0]
        max_value: $parameter.column_count_range.value.value_range[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.column_count_range.details
  column_ranges_rule:
    domain_builder:
      class_name: ColumnDomainBuilder
      column_names:

        - mainplc/WeldCurrent_Scaled
        - mainplc/SpeedDifferencePercent
        - mainplc/FormingDriveSpeedPercent
        - bsp/CalibrationDriveSpeedPercent_Scaled
      batch_request:
        datasource_name: RSM9
        data_connector_name: default_configured_data_connector_name
        data_asset_name: RSM9
        data_connector_query:
          index: -1
    parameter_builders:
      - name: min_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request: $variables.s3_batch_request
        metric_name: column.min
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
      - name: max_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request: $variables.s3_batch_request
        metric_name: column.max
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
    expectation_configuration_builders:
      - expectation_type: expect_column_min_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        column: $domain.domain_kwargs.column
        min_value: $parameter.min_range.value.value_range[0]
        max_value: $parameter.min_range.value.value_range[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.min_range.details
      - expectation_type: expect_column_max_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        column: $domain.domain_kwargs.column
        min_value: $parameter.max_range.value.value_range[0]
        max_value: $parameter.max_range.value.value_range[1]
        mostly: $variables.mostly
        meta:
          profiler_details: $parameter.max_range.details
  column_quantile_rule:
    domain_builder:
      class_name: ColumnDomainBuilder
      column_names:

        - mainplc/WeldCurrent_Scaled
        - mainplc/SpeedDifferencePercent
        - mainplc/FormingDriveSpeedPercent
        - bsp/CalibrationDriveSpeedPercent_Scaled
      batch_request:
          datasource_name: RSM9
          data_connector_name: default_configured_data_connector_name
          data_asset_name: RSM9
          data_connector_query:
            index: -1

    parameter_builders:
      - name: quantile_value_ranges
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request: $variables.s3_batch_request
        metric_name: column.quantile_values
        metric_domain_kwargs: $domain.domain_kwargs
        metric_value_kwargs:
          quantiles: $variables.quantiles
          allow_relative_error: $variables.allow_relative_error
        false_positive_rate: $variables.false_positive_rate
        num_bootstrap_samples: $variables.num_bootstrap_samples
        bootstrap_random_seed: $variables.bootstrap_random_seed
    expectation_configuration_builders:
      - expectation_type: expect_column_quantile_values_to_be_between
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        column: $domain.domain_kwargs.column
        quantile_ranges:
          quantiles: $variables.quantiles
          value_ranges: $parameter.quantile_value_ranges.value
        allow_relative_error: $variables.allow_relative_error
        meta:
          profiler_details: $parameter.quantile_value_ranges.details

"""



data_context = DataContext()

# Instantiate RuleBasedProfiler
full_profiler_config_dict: dict = yaml.safe_load(profiler_config)


rule_based_profiler: RuleBasedProfiler = RuleBasedProfiler(
    name=full_profiler_config_dict["name"],
    config_version=full_profiler_config_dict["config_version"],
    rules=full_profiler_config_dict["rules"],
    variables=full_profiler_config_dict["variables"],
    data_context=data_context,
)

expectation_suite_name="test_suite_name"
suite = rule_based_profiler.run(expectation_suite_name=expectation_suite_name)

#print(suite)


end = time.time()
executionTime = (end - start)
#print('Execution time in seconds: ' + str(executionTime))

batch_request = BatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="RSM9",
    data_connector_query={"index": -1},
)

# validator = context.get_validator(
#     batch_request=batch_request,
#     expectation_suite_name=expectation_suite_name,
# )
#validator.save_expectation_suite(suite, discard_failed_expectations=False)

context.save_expectation_suite(suite, overwrite_existing=True)

#print(validator.get_expectation_suite(discard_failed_expectations=False))


# checkpoint_config = {
#     "class_name": "SimpleCheckpoint",
#     "validations": [
#         {
#             "batch_request": batch_request,
#             "expectation_suite_name": "test_suite_name",
#         }
#     ],
# }



# my_checkpoint_name = "testcheck"  # This was populated from your CLI command.

# yaml_config = f"""
# name: {my_checkpoint_name}
# config_version: 1.0
# class_name: SimpleCheckpoint
# run_name_template: "%Y%m%d-%H%M%S-my-run-name-template"
# validations:
#   - batch_request:
#       datasource_name: RSM9
#       data_connector_name: default_configured_data_connector_name
#       data_asset_name: RSM9
#       data_connector_query:
#         index: -1
#     expectation_suite_name: test_suite_name
# """


# #context.add_checkpoint(**yaml.load(yaml_config))
# context.run_checkpoint(checkpoint_name=my_checkpoint_name)

