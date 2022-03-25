import os
import logging
from logging.config import fileConfig
from dotenv import load_dotenv, find_dotenv
from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
import warnings
import shutup
warnings.simplefilter('ignore',yaml.error.UnsafeLoaderWarning)

import pandas as pd
import pytz
import sqlalchemy


from psycopg2.errors import UniqueViolation
from omni.db import OmniParameterStoreDB, OmniDB




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


param_path = "/omni/staging/databases/mahle_behr"
omni= connect_omni_db(parameter_store_path=param_path)
model_query = """
select a.timestamp, a.value, b.title

from data_store_rawvalue as a

left join parameter_parameter as b
on a.parameter_id=b.id 
where b.title='FormingDriveSpeedPercent' or 
b.title='WeldCurrent' or
b.title='CalibrationDriveSpeedPercent'
or b.title= 'SpeedDifferencePercent'
and a.timestamp >=now()-interval '40 minutes';

 """

#model_query = "select * from data_store_rawvalue where parameter_id =UUID('78f330e9-8900-45e4-aa76-1bc412d4d65d') and timestamp >=now()-interval '10 minutes'"
with omni.conn as cur:
     result = cur.execute(model_query).fetchall()





df =pd.DataFrame(result, columns=['timestamp', 'value', 'parameter_id'])


df2=df.pivot(index='timestamp', values='value', columns='parameter_id')

print(df2.head())

df2.to_csv('/home/ncamiso.khanyile/Data/data_store_rawvalue/latestfile_time_limit.csv')