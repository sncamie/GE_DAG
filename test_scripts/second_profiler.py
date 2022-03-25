from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.rule_based_profiler import \
    RuleBasedProfiler
from ruamel import yaml

import great_expectations as ge
from great_expectations import DataContext

context = ge.get_context()


profiler_config = """
name: S3 raw profiler
config_version: 1.0

variables:
  false_positive_rate: 0.01
  mostly: 0.99

rules:
  column_count_rule:
    domain_builder:
        class_name: TableDomainBuilder
    parameter_builders:
      - name: column_count_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: RSM9
            data_connector_name: default_configured_data_connector_name
            data_asset_name: RSM9
            data_connector_query:
              index: "-6:-1"
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
      class_name: SimpleSemanticTypeColumnDomainBuilder
      semantic_types:
        - numeric
      # BatchRequest yielding exactly one batch
      batch_request:
        datasource_name: RSM9
        data_connector_name: default_configured_data_connector_name
        data_asset_name: RSM9
        data_connector_query:
          index: -1
    parameter_builders:
      - name: min_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: RSM9
            data_connector_name: default_configured_data_connector_name
            data_asset_name: RSM9
            data_connector_query:
              index: "-6:-1"
        metric_name: column.min
        metric_domain_kwargs: $domain.domain_kwargs
        false_positive_rate: $variables.false_positive_rate
        round_decimals: 2
      - name: max_range
        class_name: NumericMetricRangeMultiBatchParameterBuilder
        batch_request:
            datasource_name: RSM9
            data_connector_name: default_configured_data_connector_name
            data_asset_name: RSM9
            data_connector_query:
              index: "-6:-1"
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

print(suite)



batch_request = BatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="RSM9",
    data_connector_query={"index": -1},
)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name="test_suite_name",
)
validator.save_expectation_suite(suite, discard_failed_expectations=False)

print(validator.get_expectation_suite(discard_failed_expectations=False))


checkpoint_config = {
    "class_name": "SimpleCheckpoint",
    "validations": [
        {
            "batch_request": batch_request,
            "expectation_suite_name": "test_suite_name",
        }
    ],
}
