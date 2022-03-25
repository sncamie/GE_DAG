import os
import logging
import subprocess
from airflow.exceptions import AirflowException
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
local_directory= os.environ.get("LOCAL_DIRECTORY")
data_docs_location = "great_expectations/uncommitted/data_docs/local_site/index.html"


def validate_checkpoint(checkpoint_name):
    """Python function to validate checkpoint. Writes output to Airflow logs and
    also generates a list of Data Docs when the validation fails.

    Args:
        checkpoint_name (str): name of checkpoint to be validated against

    Raises:
        AirflowException: when validation fails. 
    """

    validation_process = subprocess.Popen(
        ["great_expectations", "--v3-api", "checkpoint", "run", checkpoint_name],
        stdout=subprocess.PIPE,
        universal_newlines=True,
    )
    validation_stdout = validation_process.communicate()[0]
    return_code = validation_process.returncode

    if return_code:
        logging.error(validation_stdout)

        

        docs_process = subprocess.Popen(
            ["great_expectations", "--v3-api", "docs", "list"],
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )
        docs_stdout = docs_process.communicate()[0]
        logging.info(docs_stdout)
        logging.info(f"file://{local_directory}/{data_docs_location}")

        raise AirflowException(
            "Checkpoint validation failed. Inspect the Data Docs for more information."
        )

    else:
        logging.info(validation_stdout)
        logging.info(f"file://{local_directory}/{data_docs_location}")


#validate_checkpoint(checkpoint_name="RSM9_checkpoint")
