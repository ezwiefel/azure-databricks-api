from unittest import TestCase

from environs import Env

from azure_databricks_api import AzureDatabricksRESTClient


class AzureDatabricksTests(TestCase):

    def __init__(self, methodName: str) -> None:
        super(AzureDatabricksTests, self).__init__(methodName=methodName)

        self.env = Env()
        self.env.read_env()

        # Use PAT Token Authorization for this
        self.pat_token = self.env.str("PAT_TOKEN")
        self.region = self.env.str("DATABRICKS_REGION")
        self.client = AzureDatabricksRESTClient(region=self.region, token=self.pat_token)