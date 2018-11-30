# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class SecretsAPI(RESTBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def scopes_create(self):
        METHOD = 'POST'
        API_PATH = '/secrets/scopes/create'

        raise NotImplementedError

    def scopes_delete(self):
        METHOD = 'POST'
        API_PATH = '/secrets/scopes/delete'

        raise NotImplementedError

    def scopes_list(self):
        METHOD = 'GET'
        API_PATH = '/secrets/scopes/list'

        raise NotImplementedError

    def list(self):
        METHOD = 'GET'
        API_PATH = '/secrets/list'

        raise NotImplementedError

    def acls_put(self):
        METHOD = 'POST'
        API_PATH = '/secrets/acls/put'

        raise NotImplementedError

    def acls_delete(self):
        METHOD = 'POST'
        API_PATH = '/secrets/acls/delete'

        raise NotImplementedError

    def acls_get(self):
        METHOD = 'GET'
        API_PATH = '/secrets/acls/get'

        raise NotImplementedError

    def acls_list(self):
        METHOD = 'GET'
        API_PATH = '/secrets/acls/list'

        raise NotImplementedError

    # Currently, putting and deleting secrets in Azure Databricks are only done for Key Vault backed
    # secrets - and only via the Azure Key Vault APIs
    # def put(self):
    #     METHOD = 'POST'
    #     API_PATH = '/secrets/put'
    #
    #     raise NotImplementedError
    #
    # def create(self):
    #     METHOD = 'POST'
    #     API_PATH = '/secrets/create'
    #
    #     raise NotImplementedError
