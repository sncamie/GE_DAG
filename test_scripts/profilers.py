import great_expectations as ge
from ruamel import yaml

from great_expectations.core.batch import BatchRequest
from great_expectations.core import ExpectationSuite

from great_expectations.rule_based_profiler.rule.rule import Rule
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler

from great_expectations.rule_based_profiler.domain_builder import (
    DomainBuilder,
    ColumnDomainBuilder,
)
from great_expectations.rule_based_profiler.parameter_builder import (
    MetricMultiBatchParameterBuilder,
)
from great_expectations.rule_based_profiler.expectation_configuration_builder import (
    DefaultExpectationConfigurationBuilder,
)




from great_expectations.rule_based_profiler.domain_builder import TableDomainBuilder


from great_expectations.core import ExpectationSuite
from great_expectations.rule_based_profiler.rule_based_profiler import RuleBasedProfiler




data_context: ge.DataContext = ge.get_context()



single_batch_batch_request: BatchRequest = BatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="RSM9",
    data_connector_query={"index": -1},
)



# domain_builder: DomainBuilder = ColumnDomainBuilder(
#     data_context=data_context,
#     batch_request=single_batch_batch_request,
# )
# domains: list = domain_builder.get_domains()



# default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
#     expectation_type="expect_column_values_to_not_be_null",
#     column="$domain.domain_kwargs.column", # Get the column from domain_kwargs that are retrieved from the DomainBuilder
# )

# simple_rule: Rule = Rule(
#     name="rule_with_no_parameters",
#     domain_builder=domain_builder,
#     expectation_configuration_builders=[default_expectation_configuration_builder],
# )




# my_rbp: RuleBasedProfiler = RuleBasedProfiler(
#     name="my_simple_rbp", data_context=data_context, config_version=1.0
# )

# my_rbp.add_rule(rule=simple_rule)


# res: ExpectationSuite = my_rbp.run()

# res.expectations





domain_builder: DomainBuilder = TableDomainBuilder(
    data_context=data_context,
    batch_request=single_batch_batch_request,
)
domains: list = domain_builder.get_domains()
domains



default_expectation_configuration_builder = DefaultExpectationConfigurationBuilder(
    expectation_type="expect_columns_to_match_set",
    column="$domain.domain_kwargs.column", # Get the column from domain_kwargs that are retrieved from the DomainBuilder
)




simple_rule: Rule = Rule(
    name="columns_to_match_set",
    domain_builder=domain_builder,
    expectation_configuration_builders=[default_expectation_configuration_builder],
)




my_rbp: RuleBasedProfiler = RuleBasedProfiler(
    name="column_set_rbp", data_context=data_context, config_version=1.0
)


my_rbp.add_rule(rule=simple_rule)



res: ExpectationSuite = my_rbp.run()



res.expectations


print (res.expectations)