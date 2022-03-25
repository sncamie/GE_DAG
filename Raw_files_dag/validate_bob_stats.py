from utils import * 

#GE required names to set up datasources
datasource_name = "bob_stats"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"


# Data Asset fields
database_conn = db_options('staging','mahle_behr')
include_schema_name = True
schema_name = "public"
table_name = "*bob_stats"
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
        "default_identifier_name": "First 1000 rows"
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
            "mean",
            "median",
            "bob_lower",
            "bob_upper",
            "created_at",
            "updated_at",
            "bob_id",
            "parameter_id",
        ]
    )
    validator.expect_table_row_count_to_be_between(max_value=153915, min_value=100)

    #Column level expectations 

    #Median 

    validator.expect_column_min_to_be_between(
        column="median", max_value=-20.0, min_value=-40.0
    )

    validator.expect_column_max_to_be_between(
        column="median", max_value=34207.0, min_value=3400.0
    )

    validator.expect_column_mean_to_be_between(
        column="median", max_value=380.0, min_value=370.0
    )

    validator.expect_column_values_to_not_be_null(column="median")


    #bob_lower

    validator.expect_column_min_to_be_between(
        column="bob_lower", max_value=-30.0, min_value=-40.0
    )

    validator.expect_column_max_to_be_between(
        column="bob_lower", max_value=34207.0, min_value=3400.0
    )

    validator.expect_column_mean_to_be_between(
        column="bob_lower", max_value=380.0, min_value=370.0
    )

    validator.expect_column_values_to_not_be_null(column="bob_lower")

    #bob_upper

    validator.expect_column_min_to_be_between(
        column="bob_upper", max_value=-30.0, min_value=-40.0
    )

    validator.expect_column_max_to_be_between(
        column="bob_upper", max_value=34412.0, min_value=3400.0
    )

    validator.expect_column_mean_to_be_between(
        column="bob_upper", max_value=381.5, min_value=380.0
    )

    validator.expect_column_values_to_not_be_null(column="bob_upper")


    validator.save_expectation_suite(discard_failed_expectations=False)


def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))
    # result = context.run_checkpoint(checkpoint_name)
    # print(f'Successful checkpoint validation: {result["success"]}\n')


connect_to_datasource()
create_expectation_suite(expectation_suite_name=expectation_suite_name)
create_checkpoint(checkpoint_name=checkpoint_name)

