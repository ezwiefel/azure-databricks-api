from unittest import expectedFailure, skipIf
from tests.base_class import AzureDatabricksTests
from azure_databricks_api.__token import TokenInfo
from azure_databricks_api.exceptions import ResourceDoesNotExist


class TestTokensAPI(AzureDatabricksTests):
    def setUp(self) -> None:
        self.token_comment = "THIS IS A TOKEN CREATED DURING CI/CD TESTING"

    def tearDown(self) -> None:
        for token in self.client.tokens.list():
            if token.comment == self.token_comment:
                self.client.tokens.revoke(token_id=token.token_id)

    def test_create(self):
        token = self.client.tokens.create(self.token_comment, lifetime_seconds=500)

        self.assertIsInstance(token, dict)
        self.assertIn('token_value', token.keys())
        self.assertIn("token_info", token.keys())
        self.assertIsInstance(token['token_info'], TokenInfo)

    def test_list(self):
        self.assertIsInstance(self.client.tokens.list()[0], TokenInfo)

    def test_revoke(self):
        token = self.client.tokens.create(self.token_comment, lifetime_seconds=500)
        self.client.tokens.revoke(token_id=token['token_info'].token_id)

    # Expected to fail until check is added to client.tokens.revoke to check if the token exists
    # The databricks API doesn't fail a call to revoke a token that doesn't exist
    @expectedFailure
    def test_revoke_not_found(self):
        with self.assertRaises(ResourceDoesNotExist):
            self.client.tokens.revoke(token_id="qjhgla4")
