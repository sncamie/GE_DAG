import os
import logging
from logging.config import fileConfig
from dotenv import load_dotenv, find_dotenv

from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest


from omni.db import OmniParameterStoreDB


context=ge.get_context()

def connect_param_store(
    env: str = "production",
    readonly: bool = True,
    omni_db_class: OmniParameterStoreDB = OmniParameterStoreDB,
) -> OmniParameterStoreDB:
    """
    Connect to the OMNI database via the parameter store.
    """
    path = f"/omni/{env}/databases/mahle_behr"
    if readonly:
        path += "/readonly"
    omni_db = omni_db_class(path)
    return omni_db

