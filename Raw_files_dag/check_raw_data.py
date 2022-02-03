from utils import * 

#GE required names to set up datasources
datasource_name = "RSM9"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"

prefix = "production_rsm9_bp/"
bucket_name = "omni-upload-mahle" 
group_names = ["data_asset_name"]
regex_pattern = "(.*)\.parquet"
reader_method = "read_parquet"
data_asset_name = "RSM9"

batch_request = BatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=data_asset_name,
    batch_spec_passthrough={
        "reader_method":reader_method,
        "reader_options": {"nrows": 1000},
    },
)

datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetS3DataConnector",
            "bucket": bucket_name,
            "prefix": prefix,
            "default_regex": {
                "pattern": regex_pattern,
                "group_names": group_names,
            },
        },
    },
}

batch_request = BatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name=data_asset_name,
    batch_spec_passthrough={
        "reader_method": reader_method,
        "reader_options": {"nrows": 1000},
    },
)

checkpoint_config = f"""\
name: {checkpoint_name}
config_version: 1
class_name: Checkpoint
validations:
- batch_request:
    datasource_name: {datasource_name}
    data_connector_name: "default_inferred_data_connector_name"
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
    """Connects to raw data in the filesystem/raw directory, and adds
    configuration to YAML file if successful.
    """

    try:
        # connect to raw data
        context.test_yaml_config(yaml.dump(datasource_config))

        # empty Expectation Test Suite, its only purpose is to validate the Datasource connection
        context.create_expectation_suite(
            expectation_suite_name="test_suite", overwrite_existing=True
        )
        validator = context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite"
        )
        print(validator.head())

    except IndexError as ex:  
        logging.exception(
            f"""Unmatched data references are not available for connection.\
Ensure that your base directory: "{prefix }", group names "{prefix}",\
and regex pattern "{regex_pattern}" are correct.
        """
        )

    except Exception as ex:
        logging.exception(
            f'Cannot connect to file in base directory "{prefix}"'
        )

    else:
        context.add_datasource(**datasource_config)
        logging.info("Added raw data file config to `great_expectations.yml` file")


def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )


    #Takble level expectations 
    validator.expect_table_row_count_to_be_between(min_value=100, max_value=2000)

    validator.expect_table_column_count_to_be_between(min_value=30, max_value=40)


    #Column level expectations 

    #Grouping Key
    validator.expect_column_values_to_be_in_set(column="grouping_key", value_set=["RSM9"])

    validator.expect_column_values_to_not_be_null(column="grouping_key")

    #CoilDiamter1
    validator.expect_column_values_to_not_be_null(column="grouping_key")

    validator.expect_column_values_to_not_be_null(column="CoilDiameter1")

    

    #LoopControl
    validator.expect_column_values_to_not_be_null(column="LoopControl")


    #CoilDiameter2
    validator.expect_column_values_to_not_be_null(column="CoilDiameter2")

    #Tag120U3
    validator.expect_column_values_to_not_be_null(column="Tag120U3")

    validator.expect_column_min_to_be_between(
        column="Tag120U3",
        max_value=-1.1431420375019752e36,
        min_value=-1.1431420375019752e36,
    )
    validator.expect_column_max_to_be_between(
        column="Tag120U3", 
        max_value=1.318592026953821e37, 
        min_value=1.318592026953821e37,
    )
    validator.expect_column_mean_to_be_between(
        column="Tag120U3", 
        max_value=1.3971577781317369e34,
        min_value=1.3971577781317369e34,
    )

    validator.expect_column_median_to_be_between(
        column="Tag120U3",
        max_value=2.7733320669515147e-33,
        min_value=2.7733320669515147e-33,
    )

    #Tag120U4
    validator.expect_column_values_to_not_be_null(column="Tag120U4")

    validator.expect_column_min_to_be_between(
        column="Tag120U4",
        max_value=-3.1986759431221336e38,
        min_value=-3.1986759431221336e38,
    )

    validator.expect_column_mean_to_be_between(
        column="Tag120U4", 
        max_value=-8.681519371946408e36,
        min_value=-8.681519371946408e36
    )

    validator.expect_column_median_to_be_between(
        column="Tag120U4", 
        max_value=-5.971155035603985e32,
        min_value=-5.971155035603985e32,
    )

    #Tag120U6
    validator.expect_column_values_to_not_be_null(column="Tag120U6")

    validator.expect_column_min_to_be_between(
        column="Tag120U6",
        max_value=-4.253530093571971e37,
        min_value=-4.253530093571971e37,
    )

    validator.expect_column_max_to_be_between(
        column="Tag120U6", 
        max_value=4.1538369916518464e34,
        min_value=4.1538369916518464e34,
    )

    validator.expect_column_mean_to_be_between(
        column="Tag120U6", 
        max_value=-4.995851165272089e34, 
        min_value=-4.995851165272089e34,
    )

    validator.expect_column_median_to_be_between(
        column="Tag120U6",
        max_value=1.5777219985211197e-30,
        min_value=1.5777219985211197e-30,
    )

    validator.save_expectation_suite(discard_failed_expectations=False)



def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))
    # result = context.run_checkpoint(checkpoint_name)
    # print(f'Successful checkpoint validation: {result["success"]}\n')


connect_to_datasource()
create_expectation_suite(expectation_suite_name=expectation_suite_name)
create_checkpoint(checkpoint_name=checkpoint_name)