from environs import Env
from azure_databricks_api import AzureDatabricksRESTClient


def create_client():
    env = Env()
    env.read_env()

    # Use PAT Token Authorization for this
    pat_token = env.str("PAT_TOKEN")
    region = env.str("DATABRICKS_REGION")
    return AzureDatabricksRESTClient(region=region, token=pat_token)
