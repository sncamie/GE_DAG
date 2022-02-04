import pandas as pd
from great_expectations.data_context.types.base import DataContextConfig, S3StoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from great_expectations.profile import BasicSuiteBuilderProfiler
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier



df = pd.read_parquet("s3://omni-upload-mahle/production_rsm9_bsp/2020-07-08 02:40:57--2020-07-08 02:55:58.parquet")
print(df.head())
#GE required names to set up datasources
datasource_name = "RSM9"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"
expectaction_store_name = f"{datasource_name}_expectation_store"
validation_store_name = f"{datasource_name}_validation_store"
prefix = "production_rsm9_bp/"
bucket_name = "omni-upload-mahle" 
group_names = ["data_asset_name"]
regex_pattern = "(.*)\.parquet"
reader_method = "read_parquet"
data_asset_name = "RSM9"






#Initiate great_expectations context
data_context_config = DataContextConfig(
    config_version=2,
    plugins_directory=None,
    config_variables_file_path=None,
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
                "group_names": ["data_asset_name"],
            },
        },
    },
   },
    stores={
        expectaction_store_name : {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": bucket_name, 
                "prefix": prefix, 
            },
        },
        validation_store_name : {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket":bucket_name,
                "prefix": prefix,
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name=expectaction_store_name ,
    validations_store_name=validation_store_name ,
    evaluation_parameter_store_name="evaluation_parameter_store",
    store_backend_defaults=S3StoreBackendDefaults(default_bucket_name=bucket_name),
)


context = BaseDataContext(project_config=data_context_config)


batch_kwargs = {'dataset': df, 'datasource': datasource_name}

expectation_suite_name = expectation_suite_name

suite = context.create_expectation_suite(expectation_suite_name, overwrite_existing=True)

#Here where you can define expectations for your table columns
validator = context.get_batch(batch_kwargs, suite)
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

#Include all the columns to be profiled
included_columns = ["Tag120U4","Tag120U6","Tag120U3", "CoilDiameter2","LoopControl","grouping_key"]
scaffold_config = {
    "included_columns": included_columns,
}
# Build your suites
suite, evr = BasicSuiteBuilderProfiler().profile(validator,profiler_configuration=scaffold_config)

#Save GE demo file
context.save_expectation_suite(suite, expectation_suite_name)

#Build GE data docs
context.build_data_docs()

print("Expecations well generated and stored on S3 bucket !")
