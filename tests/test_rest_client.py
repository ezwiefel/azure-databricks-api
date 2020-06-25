from azure_databricks_api import AzureDatabricksRESTClient
from azure_databricks_api.exceptions import AuthorizationError
from environs import Env

import pytest

env = Env()
env.read_env()

REGION = env.str("DATABRICKS_REGION")
PAT_TOKEN = env.str("PAT_TOKEN")


def test_pat_token_auth():
    client = AzureDatabricksRESTClient(region=REGION, token=PAT_TOKEN)

    client.dbfs.list("/")


def test_wrong_pat_token_raises_error():
    client = AzureDatabricksRESTClient(region=REGION, token="WRONGTOKEN")

    with pytest.raises(AuthorizationError):
        client.dbfs.list('/')
