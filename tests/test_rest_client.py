from unittest import TestCase, expectedFailure
from azure_databricks_api import AzureDatabricksRESTClient
from azure_databricks_api.exceptions import AuthorizationError
from environs import Env


class TestAzureDatabricksRESTClient(TestCase):

    def setUp(self) -> None:
        self.env = Env()
        self.env.read_env()

        self.region = self.env.str("DATABRICKS_REGION")
        self.pat_token = self.env.str("PAT_TOKEN")

    def test_pat_token_auth(self):
        client = AzureDatabricksRESTClient(region=self.region, token=self.pat_token)

        client.dbfs.list('/')

    def test_wrong_pat_token(self):
        client = AzureDatabricksRESTClient(region=self.region, token='Thisisthewrongtoken')

        with self.assertRaises(AuthorizationError):
            client.dbfs.list('/')