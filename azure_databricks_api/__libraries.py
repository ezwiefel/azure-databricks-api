# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase


class LibrariesAPI(RESTBase):

    def __init__(self, **kwargs):
        super(LibrariesAPI, self).__init__(**kwargs)

    def all_cluster_statuses(self):
        METHOD = 'GET'
        API_PATH = '/libraries/all-cluster-statuses'

        raise NotImplementedError

    def cluster_status(self):
        METHOD = 'GET'
        API_PATH = '/libraries/cluster-status'

        raise NotImplementedError

    def install(self):
        METHOD = 'POST'
        API_PATH = '/libraries/install'

        raise NotImplementedError

    def uninstall(self):
        METHOD = 'POST'
        API_PATH = '/libraries/uninstall'

        raise NotImplementedError