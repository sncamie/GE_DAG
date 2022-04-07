#Predifined variables for each data source, suite, checkpoint,etc
datasource_name = "bobs"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"

prefix = "validation_files/"
bucket_name = "omni-upload-mahle" 
group_names = ["bobs"]
regex_pattern = "(.*)\.csv"
reader_method = "read_csv"
data_asset_name = "bobs"



#Datasource configuration

datasource_config = {
    "name": datasource_name,
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {            
	        "class_name": "RuntimeDataConnector",            
	        "batch_identifiers": ["default_identifier_name"],     
    },
        "default_configured_data_connector_name": {
            "class_name": "ConfiguredAssetS3DataConnector",
            "bucket": bucket_name,
            "prefix": prefix,
            "assets":{
                data_asset_name:{
                    "group_names":group_names,
                    "pattern":regex_pattern,
                },

            },

            
        },
    },
}
#calling a batch of data from our data source, using index -1 to get the latest file. Can be any python slice
batch_request = BatchRequest(
    datasource_name=datasource_name,
    data_connector_name="default_configured_data_connector_name",
    data_asset_name=data_asset_name,
    data_connector_query= {"index":-1},
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
