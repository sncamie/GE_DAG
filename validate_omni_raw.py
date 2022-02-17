from utils import *

#GE required names to set up datasources
# Fill in information for the fields specified in all-caps
datasource_name = "omni_raw"
expectation_suite_name= f"{datasource_name }_suite"
checkpoint_name = f"{datasource_name}_checkpoint"

# Data Asset fields
base_directory = "/home/ncamiso.khanyile/Data/" 
group_names = ["data_asset_name"]
regex_pattern = "(.*)\.csv"
data_asset_name = "raw"
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
                    "base_directory": "data_store_rawvalue/",
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
    data_connector_query={"index":-1},
    batch_spec_passthrough={
        "reader_method": reader_method,
     
    },
)

#conifgure checkpoint 

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
            Ensure that your base directory: "{base_directory}", group names "{group_names}",\
            and regex pattern "{regex_pattern}" are correct.
        """
        )

    except Exception as ex:
        logging.exception(
            f'Cannot connect to file in base directory "{base_directory}"'
        )

    else:
        context.add_datasource(**datasource_config)
        logging.info("Added raw data file config to `great_expectations.yml` file")



def create_expectation_suite(expectation_suite_name):

    context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=expectation_suite_name
    )

    #column level expectations 

    validator.expect_column_values_to_be_between(
        column= "CalibrationDriveSpeedPercent",
        min_value= -0.1280487775802612,
        max_value= 64.65243530273438,
    )

    validator.expect_column_values_to_be_between(
        column= "SpeedDifferencePercent",
        min_value= -0.2317073196172714,
        max_value= 40.024391174316406,
    )

    validator.expect_column_values_to_be_between(
        column= "FormingDriveSpeedPercent",
        min_value=-0.170731708407402,
        max_value=60.0,
    )

    validator.expect_column_values_to_be_between(
        column= "WeldCurrent",
        min_value=0.0,
        max_value=3.8,
    )

    validator.save_expectation_suite(discard_failed_expectations=False)

def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))
    result = context.run_checkpoint(checkpoint_name)
    print(f'Successful checkpoint validation: {result["success"]}\n')




connect_to_datasource()
create_expectation_suite(expectation_suite_name=expectation_suite_name)
create_checkpoint(checkpoint_name=checkpoint_name)