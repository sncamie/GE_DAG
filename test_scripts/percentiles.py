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
name: s3 quantiles
class_name: RuleBasedProfiler
config_version: 1.0

variables:
  quantiles:
    - 2.5e-1
    - 5.0e-1
    - 7.5e-1
  allow_relative_error: linear
  num_bootstrap_samples: 9139
  bootstrap_random_seed: 43792
  batch_request:
    datasource_name: RSM9
    data_connector_name: default_configured_data_connector_name
    data_asset_name: RSM9
    data_connector_query:
      index: "-5:-1"

  false_positive_rate: 5.0e-2

rules:
  column_quantiles_rule:
    domain_builder:
      class_name: ColumnDomainBuilder
      include_column_names:
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
        batch_request: $variables.batch_request
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

expectation_suite_name="test3_suite_name"
suite = rule_based_profiler.run(expectation_suite_name=expectation_suite_name)