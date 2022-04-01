from great_expectations.core.batch import BatchRequest
import great_expectations as ge
from great_expectations.datasource.new_datasource import Datasource

batch_request= BatchRequest(
    datasource_name="RSM9",
    data_connector_name="default_configured_data_connector_name",
    data_asset_name="RSM9",
    data_connector_query= {"index": "-11:-1"},
    batch_spec_passthrough = {"sampling_method":"_sample_using_random", "sampling_kwargs":{"p": 0.2} },

)


context= ge.get_context()
datasource= context.get_datasource("RSM9")
#batch_request.batch_spec_passthrough["sampling_method"] = "_sample_using_random"
#batch_request.batch_spec_passthrough["sampling_kwargs"] = {"p": 1.0e-1}
batch_list=datasource.get_batch_list_from_batch_request(batch_request=batch_request)

print(len(batch_list))