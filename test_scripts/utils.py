import logging
import os
import warnings
from logging.config import fileConfig

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from ruamel import yaml

warnings.simplefilter("ignore", yaml.error.UnsafeLoaderWarning)

import pandas as pd
import pytz
import sqlalchemy
from omni.db import OmniDB, OmniParameterStoreDB
from psycopg2.errors import UniqueViolation


context = ge.get_context()


def connect_to_datasource(datasource_config: str) -> str:
    """The Datasource provides an interface for a Data Connector and an Execution Engine
    to work together and proper communication between Great Expectations and your source data systems.
    configuration for each type of Datasource is slightly different and must be defined in the validation step.

     e.g. s3 vs filesystem etc.


    :type datasource_config: str
    :return: A datasource for the project
    :rtype: str
    """

    context.test_yaml_config(yaml.dump(datasource_config))
    # context.create_expectation_suite(
    #     expectation_suite_name="test_suite", overwrite_existing=True
    #     )
    # validator = context.get_validator(
    #     batch_request=batch_request, expectation_suite_name="test_suite"
    #     )
    # print(validator.head())

    context.add_datasource(**datasource_config)


def create_checkpoint(checkpoint_name: str, checkpoint_config: str) -> str:
    """Creates a checkpoint, which will be called by PythonOperator
    in dag to run validation. The name is defined in the same manner as the datasource
    and expectation suite. Stored in great_expectations/checkpoints/


    :type checkpoint_name: str
    :return: a checkpoint
    :rtype: str
    """

    context.test_yaml_config(yaml_config=checkpoint_config, pretty_print=True)
    context.add_checkpoint(**yaml.load(checkpoint_config))


def connect_omni_db(**kwargs) -> OmniDB:
    """
    Connect to OMNI database using received kwargs.

    Looks to use parameter store string by default, and
    uses explicit db credentials otherwise.

    Provide either
    --------------
    :param parameter_store_path: S3 parameter store path. 1/1.
        E.g. "/omni/staging/databases/mahle_behr"

        NOTE: In order for the `parameter_store_path` key
        to work, the relevant value must be stored in the
        AWS Systems Manager, and you must have the relevant
        credentials to access/decrypt the data in the store.

    or:

    :param omni_host: Name of DB host server. 1/4.
    :param omni_dbname: Name of DB to access. 2/4.
    :param omni_user: Name of user trying to access DB. 3/4.
    :param omni_password: Pwd for accessing DB. 4/4.

    :return: OmniDB connection object
    NOTE: OmniParameterStoreDB inherits directly from OmniDB,
    so `isinstance(OmniParameterStoreDB("..."), OmniDB)` is `True`.
    """
    if kwargs.get("parameter_store_path", None):
        logging.debug("Connecting to OMNI via parameter store")

        omni_db = OmniParameterStoreDB(kwargs["parameter_store_path"])
    else:
        logging.debug("Connecting to OMNI via database details")

        omni_db = OmniDB(
            host=kwargs["omni_host"],
            database=kwargs["omni_dbname"],
            username=kwargs["omni_user"],
            password=kwargs["omni_password"],
        )
    logging.info("Connected to OmniDB!")
    return omni_db