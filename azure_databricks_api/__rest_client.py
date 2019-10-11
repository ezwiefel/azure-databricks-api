# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__clusters import ClusterAPI
from azure_databricks_api.__groups import GroupsAPI
from azure_databricks_api.__token import TokensAPI
from azure_databricks_api.__workspace import WorkspaceAPI
from azure_databricks_api.__dbfs import DbfsAPI
from azure_databricks_api.__libraries import LibrariesAPI

class AzureDatabricksRESTClient(object):

    """
    API List:
        Instance Profiles
            Profiles Add
            Profiles List
            Profiles Remove
    """

    def __init__(self, region, token):
        self._region = region
        self._token = token
        self._host = 'https://{region}.azuredatabricks.net'.format(region=self._region)
        self.api_version = '2.0'

        parameters = {'host': self._host, 'api_version': self.api_version, 'token': self._token}

        self.clusters = ClusterAPI(**parameters)
        self.groups = GroupsAPI(**parameters)
        self.tokens = TokensAPI(**parameters)
        self.workspace = WorkspaceAPI(**parameters)
        self.dbfs = DbfsAPI(**parameters)
        self.libraries = LibrariesAPI(**parameters)