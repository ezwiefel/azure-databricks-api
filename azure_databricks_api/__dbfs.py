# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class DbfsAPI(RESTBase):

    def __init__(self, **kwargs):
        super(DbfsAPI, self).__init__(**kwargs)

    def add_block(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/add-block'

        raise NotImplementedError

    def close(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/close'

        raise NotImplementedError

    def create(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/create'

        raise NotImplementedError

    def delete(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/delete'

        raise NotImplementedError

    def get_status(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/get-status'

        raise NotImplementedError

    def list(self):
        METHOD = 'GET'
        API_PATH = '/dbfs/list'

        raise NotImplementedError

    def mkdirs(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/mkdirs'

        raise NotImplementedError

    def move(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/move'

        raise NotImplementedError

    def put(self):
        METHOD = 'POST'
        API_PATH = '/dbfs/put'

        raise NotImplementedError

    def read(self):
        METHOD = 'GET'
        API_PATH = '/dbfs/read'

        raise NotImplementedError