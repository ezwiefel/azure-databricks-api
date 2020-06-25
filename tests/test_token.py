import pytest

from tests.utils import create_client

from azure_databricks_api.__token import TokenInfo
from azure_databricks_api.exceptions import ResourceDoesNotExist

TOKEN_COMMENT = "THIS IS A TOKEN CREATED DURING THE CI/CD TESTING"
CREATED_TOKEN_ID = ""

client = create_client()


def teardown_module(module):
    for token in client.tokens.list():
        if token.comment == TOKEN_COMMENT:
            client.tokens.revoke(token_id=token.token_id)


def test_create():
    global CREATED_TOKEN_ID

    token = client.tokens.create(comment=TOKEN_COMMENT, lifetime_seconds=500)

    CREATED_TOKEN_ID = token['token_info'].token_id

    assert isinstance(token, dict)
    assert "token_value" in token.keys()
    assert "token_info" in token.keys()

    assert isinstance(token['token_info'], TokenInfo)


def test_list():
    token_list = client.tokens.list()

    assert isinstance(token_list[0], TokenInfo)
    assert TOKEN_COMMENT in [token.comment for token in token_list]


def test_revoke():
    global CREATED_TOKEN_ID

    client.tokens.revoke(token_id=CREATED_TOKEN_ID)