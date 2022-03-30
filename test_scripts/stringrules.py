from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler
from ruamel import yaml
import time
import great_expectations as ge
from great_expectations import DataContext
from great_expectations.checkpoint import SimpleCheckpoint

context = ge.get_context()

start = time.time()

profiler_config = """
name: S3 raw profiler
config_version: 1.0

variables:
  false_positive_rate: 0.01
  mostly: 0.99
  batch_request:
    datasource_name: RSM9
    data_connector_name: default_configured_data_connector_name
    data_asset_name: RSM9
    data_connector_query:
        index: -1

rules:
  column_set_rule:
    domain_builder:
        class_name: ColumnDomainBuilder
        module_name: great_expectations.rule_based_profiler.domain_builder
        batch_request: $variables.batch_request

        include_column_names:
            - mainplc/WeldCurrent_Scaled
            - mainplc/SpeedDifferencePercent
            - mainplc/FormingDriveSpeedPercent
            - bsp/CalibrationDriveSpeedPercent_Scaled
    parameter_builders:
      - name: column_value_set
        class_name: ValueSetMultiBatchParameterBuilder
        batch_request: $variables.batch_request
        module_name: great_expectations.rule_based_profiler.parameter_builder
        metric_domain_kwargs: $domain.domain_kwargs
    expectation_configuration_builders:
      - expectation_type: expect_column_values_to_be_in_set
        class_name: DefaultExpectationConfigurationBuilder
        module_name: great_expectations.rule_based_profiler.expectation_configuration_builder
        column: $domain.domain_kwargs
        value_set: $parameter.column_value_set.value

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

expectation_suite_name = "string_suite"
suite = rule_based_profiler.run(expectation_suite_name=expectation_suite_name)

context.save_expectation_suite(suite, overwrite_existing=True)
