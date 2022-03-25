from utils import *

#GE required names to set up datasources
# Fill in information for the fields specified in all-caps
datasource_name = "prescribe"
expectation_suite_name= f"{datasource_name }_suite"
checkpoint_name = f"{datasource_name}_checkpoint"

# Data Asset fields
base_directory = "/home/ncamiso.khanyile/Data/prescribe_prescribereportdata/" 
group_names = ["data_asset_name"]
regex_pattern = "(.*)\.csv"
data_asset_name = "prescribe"
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
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "module_name": "great_expectations.datasource.data_connector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetFilesystemDataConnector",
            "base_directory": base_directory,
            "default_regex": {"group_names": group_names, "pattern": regex_pattern},
        },
    },
}
#Create in memory data frame
model_query = """
select a.id as id, a.bob_upper as bob_upper, a.bob_lower as bob_lower,a.bob_median as bob_median,a.bob_target as bob_target, b.title
from prescribe_prescribereportdata as a
left join parameter_parameter as b
on a.parameter_id=b.id 
where b.title='FormingDriveSpeedPercent' or 
b.title='WeldCurrent' or
b.title='CalibrationDriveSpeedPercent'
or b.title= 'SpeedDifferencePercent'

 """

with omni.conn as cur:
     result = cur.execute(model_query).fetchall()



df =pd.DataFrame(result,columns=['id','bob_upper', 'bob_lower', 'bob_median','bob_target', 'parameter_id'])



df3=df.pivot(index='id', values=['bob_upper', 'bob_lower', 'bob_median','bob_target'], columns='parameter_id')


d= df3.columns.swaplevel().map('_'.join)
df3.columns=df3.columns.droplevel(0)
df3.columns=d



batch_request = RuntimeBatchRequest(
    datasource_name=datasource_name ,
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name=data_asset_name,  
    runtime_parameters={"batch_data": df3},  
    batch_identifiers={"default_identifier_name": "default_identifier"},
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

    #bob upper

    #CalibrationDriveSpeedPercent_bob_upper

    validator.expect_column_values_to_be_between(
        column= "CalibrationDriveSpeedPercent_bob_upper",
        min_value= 21.0,
        max_value= 66.2073135375977,
    )

    #FormingDriveSpeedPercent_bob_upper

    validator.expect_column_values_to_be_between(
    column= "FormingDriveSpeedPercent_bob_upper",
    min_value= 19.4695129394531,
    max_value= 58.0,
    )


    #SpeedDifferencePercent_bob_upper
    validator.expect_column_values_to_be_between(
    column= "SpeedDifferencePercent_bob_upper",
    min_value= 1.84756088256836,
    max_value= 18.4207344055176,
    )

    #WeldCurrent_bob_upper
    validator.expect_column_values_to_be_between(
    column= "WeldCurrent_bob_upper",
    min_value= 2.36979150772095,
    max_value= 3.79340267181396,
    )


    #bob lower 

    
    #CalibrationDriveSpeedPercent_bob_lower

    validator.expect_column_values_to_be_between(
        column= "CalibrationDriveSpeedPercent_bob_lower",
        min_value=21.0243911743164,
        max_value= 60.3170738220215,
    )

    #FormingDriveSpeedPercent_bob_lower

    validator.expect_column_values_to_be_between(
    column= "FormingDriveSpeedPercent_bob_lower",
    min_value= 18.4756088256836,
    max_value= 53.9756088256836,
    )


    #SpeedDifferencePercent_bob_lower
    validator.expect_column_values_to_be_between(
    column= "SpeedDifferencePercent_bob_lower",
    min_value= 1.61585426330566,
    max_value= 14.9573173522949,
    )

    #WeldCurrent_bob_lower
    validator.expect_column_values_to_be_between(
    column= "WeldCurrent_bob_lower",
    min_value= 2.33940982818604,
    max_value= 3.61111092567444,
    )

    #bob median 

    
    #CalibrationDriveSpeedPercent_bob_median

    validator.expect_column_values_to_be_between(
        column= "CalibrationDriveSpeedPercent_bob_median",
        min_value= 21.2195129394531,
        max_value= 62.8170738220215,
    )

    #FormingDriveSpeedPercent_bob_median

    validator.expect_column_values_to_be_between(
    column= "FormingDriveSpeedPercent_bob_median",
    min_value= 19.0731716156006,
    max_value= 56.4695129394531,
    )


    #SpeedDifferencePercent_bob_median
    validator.expect_column_values_to_be_between(
    column= "SpeedDifferencePercent_bob_median",
    min_value= 1.80487632751465,
    max_value= 16.6890258789062,
    )

    #WeldCurrent_bob_median
    validator.expect_column_values_to_be_between(
    column= "WeldCurrent_bob_median",
    min_value= 2.36111116409302,
    max_value= 3.68489599227905,
    )

    #bob target 

    
    #CalibrationDriveSpeedPercent_bob_target

    validator.expect_column_values_to_be_between(
        column= "CalibrationDriveSpeedPercent_bob_target",
        min_value= 21.2195129394531,
        max_value= 62.8170738220215,
    )

    #FormingDriveSpeedPercent_bob_target

    validator.expect_column_values_to_be_between(
    column= "FormingDriveSpeedPercent_bob_target",
    min_value= 19.0731716156006,
    max_value= 56.4695129394531,
    )


    #SpeedDifferencePercent_bob_target
    validator.expect_column_values_to_be_between(
    column= "SpeedDifferencePercent_bob_target",
    min_value= 1.80487632751465,
    max_value= 16.6890258789062,
    )

    #WeldCurrent_bob_target
    validator.expect_column_values_to_be_between(
    column= "WeldCurrent_bob_target",
    min_value= 2.36111116409302,
    max_value= 3.68489599227905,
    )

    validator.save_expectation_suite(discard_failed_expectations=False)

def create_checkpoint(checkpoint_name):

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))
    #result = context.run_checkpoint(checkpoint_name)
    #print(f'Successful checkpoint validation: {result["success"]}\n')




connect_to_datasource()
create_expectation_suite(expectation_suite_name=expectation_suite_name)
create_checkpoint(checkpoint_name=checkpoint_name)


