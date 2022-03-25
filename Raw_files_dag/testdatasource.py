from utils import * 

#GE required names to set up datasources
datasource_name = "bob_stats"
expectation_suite_name = f"{datasource_name}_suite"
checkpoint_name = f"{datasource_name}_checkpoint"


param_path = "/omni/staging/databases/mahle_behr"
omni = connect_omni_db(parameter_store_path=param_path)
# Data Asset fields

database_conn = OmniDB(
            host="omni_host",
            database="mahle_behr",
            username="omni_user",
            password="omni_password",
        )
#database_conn = omni.conn
include_schema_name = True
schema_name = "public"
table_name = "bob_stats"
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


context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)