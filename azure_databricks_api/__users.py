# Copyright (c) 2018 Microsoft
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class UsersAPI(RESTBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_user(self, admin=False, cluster_create=False, groups=None):
        METHOD = 'POST'
        API_PATH = '/preview/scim/v2/Users'

        raise NotImplementedError

