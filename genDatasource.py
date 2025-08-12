from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

import great_expectations as ge

context = ge.get_context()

datasource_config = {
    "name": "demo2_datasource",
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
            "base_directory": "C:/repos/fusion-analytics-workbench/Greatexpectation\FilesValidation/DynamicValidation/files",
            "default_regex": {"group_names": ["data_asset_name"], "pattern": "(.*)"},
        },
    },
}

# test config
context.test_yaml_config(yaml.dump(datasource_config))

# context.add_datasource(**datasource_config)

# verify
# Here is a RuntimeBatchRequest using a path to a single CSV file
batch_request = RuntimeBatchRequest(
    datasource_name="demo2_datasource",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="File1",  # This can be anything that identifies this data_asset for you
    runtime_parameters={
        "path": "demoDynamicValidation/files/File1.csv"
    },  # Add your path here.
    batch_identifiers={"default_identifier_name": "default_identifier"},
)

# Please note this override is only to provide good UX for docs and tests.
# In normal usage you'd set your path directly in the BatchRequest above.
batch_request.runtime_parameters["path"] = "demoDynamicValidation/files/File1.csv"

context.create_expectation_suite(
    expectation_suite_name="test_suite", overwrite_existing=True
)
validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="test_suite"
)
print(validator.head())

# NOTE: The following code is only for testing and can be ignored by users.
assert isinstance(validator, ge.validator.validator.Validator)
