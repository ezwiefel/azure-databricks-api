# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class TokensAPI(RESTBase):

    def __init__(self, **kwargs):
        super(TokensAPI, self).__init__(**kwargs)

    def create(self):
        METHOD = 'POST'
        API_PATH = '/tokens/create'

        raise NotImplementedError

    def list(self):
        METHOD = 'GET'
        API_PATH = '/tokens/list'

        raise NotImplementedError

    def revoke(self):
        METHOD = 'POST'
        API_PATH = '/tokens/revoke'

        raise NotImplementedError