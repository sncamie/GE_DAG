import json
import great_expectations as ge
from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler

jsonschema_file = "geschema.json"
suite_name = "new_suite"

context = ge.data_context.DataContext()

# with open (jsonschema_file, 'r') as j:
#     contents=json.loads(j.read())
#     print(contents)





with open(jsonschema_file, "r") as f:
    raw_json = f.read()
    schema = json.loads(raw_json, strict=False)

print("Generating suite...")
profiler = JsonSchemaProfiler()
suite = profiler.profile(schema, suite_name)
context.save_expectation_suite(suite)