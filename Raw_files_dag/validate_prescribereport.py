from utils import * 


from utils import * 

#GE required names to set up datasources
datasource_name = "prescribe_report"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"


# Data Asset fields
database_conn = connect_param_store("staging")
include_schema_name = True
schema_name = "public"
table_name = "*prescribereport"
data_asset_name = f"{schema_name}.{table_name}" if include_schema_name else table_name


datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": database_conn,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "include_schema_name": include_schema_name ,
        },
    },
}

batch_request = RuntimeBatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name=data_asset_name,
    runtime_parameters={"query": f"SELECT * FROM {data_asset_name} LIMIT 1000"},
    batch_identifiers={
        "default_identifier_name": "First 1000 rows for profiling retail source data"
    },
)

checkpoint_config = f"""\
name: {checkpoint_name}
config_version: 1
class_name: Checkpoint
validations:
- batch_request:
    datasource_name: {datasource_name}
    data_connector_name: default_inferred_data_connector_name
    data_asset_name: {data_asset_name}
    data_connector_query:
        index: -1
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

def connect_to_datasource():
    """Connects to Postgres source database, adding to YAML file if successful."""

    try:
        # connect to the datasource and sample out about 1000 rows just to confirm
        context.test_yaml_config(yaml.dump(datasource_config))

        # empty Expectation Test Suite, its only purpose is to validate the Datasource connection
        context.create_expectation_suite(
            expectation_suite_name="test_suite", overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite"
        )
        print(validator.head())

    except Exception as ex:
        # the raised error may be completely off the mark, saying that password authentication
        # failed even though password is correct, but port number, host name or DB name could be wrong.
        logging.exception(
            f"Cannot connect to database with connection string {database_conn}"
        )

    else:
        context.add_datasource(**datasource_config)
        logging.info("Added database config to `great_expectations.yml`")



def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )

    #Table level expectations 

    validator.expect_table_columns_to_match_ordered_list(
        column_list=[
            "id",
            "controllable",
            "in_bob",
            "hidden",
            "hidden_reason",
            "importance",
            "latest_param_value",
            "latest_param_timestamp",
            "bob_lower",
            "bob_upper",
            "bob_target",
            "bob_mean",
            "bob_median",
            "period_mean",
            "period_median",
            "period_timeseries",
            "period_histogram",
            "override_value",
            "parameter_id",
            "prescribe_report_id",
            "trained_model_id",
        ]
    )
    validator.expect_table_row_count_to_be_between(max_value=1000, min_value=100)

    #Column level expectations 

    #bob_lower 

    validator.expect_column_min_to_be_between(
        column="bob_lower", max_value=-30.0, min_value=-33.0
    )

    validator.expect_column_max_to_be_between(
        column="bob_lower", max_value=10371.0, min_value=10271.0
    )

    validator.expect_column_mean_to_be_between(
        column="bob_lower", max_value=195.2, min_value=195.0
    )
    validator.expect_column_values_to_not_be_null(column="bob_lower")

    #bob_upper

    validator.expect_column_min_to_be_between(
        column="bob_upper", max_value=-30.0, min_value=-33.0
    )

    validator.expect_column_max_to_be_between(
        column="bob_upper", max_value=10380.0, min_value=10280.0
    )
    validator.expect_column_mean_to_be_between(
        column="bob_upper", max_value=196.2, min_value=196.0
    )
    validator.expect_column_values_to_not_be_null(column="bob_upper")

    #bob_target

    validator.expect_column_min_to_be_between(
        column="bob_target", max_value=-30.0, min_value=-33.0
    )

    validator.expect_column_max_to_be_between(
        column="bob_target", max_value=10380.0, min_value=10280.0
    )

    validator.expect_column_mean_to_be_between(
        column="bob_target", max_value=195.8, min_value=195.0
    )
    validator.expect_column_values_to_not_be_null(column="bob_target")

    #period_mean

    validator.expect_column_min_to_be_between(
        column="period_mean", max_value=-30.0, min_value=-30.3
    ) 

    validator.expect_column_max_to_be_between(
        column="period_mean", max_value=19915.0, min_value=19815.0
    )

    validator.expect_column_mean_to_be_between(
        column="period_mean", max_value=260.0, min_value=259.0
    )
    validator.expect_column_values_to_not_be_null(column="period_mean")


    

