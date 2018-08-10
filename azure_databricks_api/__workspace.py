# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class WorkspaceAPI(RESTBase):

    def __init__(self, **kwargs):
        super(WorkspaceAPI, self).__init__(**kwargs)

    def delete(self):
        METHOD = 'POST'
        API_PATH = '/workspace/delete'

        raise NotImplementedError

    def export(self):
        METHOD = 'GET'
        API_PATH = '/workspace/export'

        raise NotImplementedError

    def get_status(self):
        METHOD = 'GET'
        API_PATH = '/workspace/get-status'

        raise NotImplementedError

    def workspace_import(self):
        METHOD = 'POST'
        API_PATH = '/workspace/import'

        raise NotImplementedError

    def list(self):
        METHOD = 'GET'
        API_PATH = '/workspace/list'

        raise NotImplementedError

    def mkdirs(self):
        METHOD = 'POST'
        API_PATH = '/workspace/mkdirs'

        raise NotImplementedError