from great_expectations.core.batch import BatchRequest
import great_expectations as ge
from great_expectations.datasource.new_datasource import Datasource
from great_expectations.expectations.metrics.import_manager import sa




data_connector_query_multi_index = {    
    "index":  "-1:-8",
    }

data_connector_query_last_index = { "index": -1,}

batch_request= BatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="RSM9",
    data_connector_query= {
        "index":{"-20:-10"}
    },
    batch_spec_passthrough = {"sampling_method":"_sample_using_random", "sampling_kwargs":{"p": 0.1} },

)


context= ge.get_context()
datasource= context.get_datasource("RSM9")
#batch_request.batch_spec_passthrough["sampling_method"] = "_sample_using_random"
#batch_request.batch_spec_passthrough["sampling_kwargs"] = {"p": 1.0e-1}
batch_list=context.get_batch_list(batch_request=batch_request)
batch_data= batch_list[0].data

# num_rows = batch_data.execution_engine.engine.execute(
#     sa.select([sa.func.count()]).select_from(batch_data.selectable)
# ).scalar()


print(len(batch_list))