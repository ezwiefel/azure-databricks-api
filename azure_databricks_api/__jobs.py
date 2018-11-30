# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class JobsAPI(RESTBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create(self):
        METHOD = 'POST'
        API_PATH = '/jobs/create'

        raise NotImplementedError

    def list(self):
        METHOD = 'GET'
        API_PATH = '/jobs/list'

        raise NotImplementedError

    def delete(self):
        METHOD = 'POST'
        API_PATH = '/jobs/delete'

        raise NotImplementedError

    def get(self):
        METHOD = 'GET'
        API_PATH = '/jobs/get'

        raise NotImplementedError

    def reset(self):
        METHOD = 'POST'
        API_PATH = '/jobs/reset'

        raise NotImplementedError

    def run_now(self):
        METHOD = 'POST'
        API_PATH = '/jobs/run-now'

        raise NotImplementedError

    def runs_submit(self):
        METHOD = 'POST'
        API_PATH = '/jobs/runs/submit'

        raise NotImplementedError

    def runs_list(self):
        METHOD = 'GET'
        API_PATH = '/jobs/runs/list'

        raise NotImplementedError

    def runs_get(self):
        METHOD = 'GET'
        API_PATH = '/jobs/runs/get'

        raise NotImplementedError

    def runs_export(self):
        METHOD = 'GET'
        API_PATH = '/jobs/runs/export'

        raise NotImplementedError

    def runs_cancel(self):
        METHOD = 'POST'
        API_PATH = '/jobs/runs/cancel'

        raise NotImplementedError

    def runs_get_output(self):
        METHOD = 'GET'
        API_PATH = '/jobs/runs/get-output'

        raise NotImplementedError

    def runs_delete(self):
        METHOD = 'POST'
        API_PATH = '/jobs/runs/delete'

        raise NotImplementedError
