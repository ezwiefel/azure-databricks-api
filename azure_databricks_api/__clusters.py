# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import collections

from azure_databricks_api.__base import RESTBase
from azure_databricks_api.__utils import dict_update
from azure_databricks_api.exceptions import ResourceDoesNotExist, APIError, AuthorizationError, ERROR_CODES


class ClusterAPI(RESTBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create(self, cluster_name, num_workers, spark_version, node_type_id,
               python_version=3, autotermination_minutes=60, custom_spark_version=False, **kwargs):
        """
        Creates a new cluster in the given
        Parameters
        ----------
        cluster_name : str
            The display name of the cluster being created

        num_workers : int
            The number of worker nodes in the cluster

        spark_version : str
        node_type_id : str

        python_version : int, optional, default=3
            

        autotermination_minutes : int, optional, default=60
            Automatically terminates the cluster after it is inactive for this time in minutes.
            If not set, this cluster will not be automatically terminated. If specified, the threshold
            must be between 10 and 10000 minutes. You can also set this value to 0 to explicitly disable
            automatic termination.

        custom_spark_version : bool, optional, default=False
            If a custom Spark version is passed - then this prevents error checking for supported Spark versions

        kwargs : optional
            Other keyword arguments are passed to the API in the JSON payload. See supported arguments here:
            https://docs.azuredatabricks.net/api/latest/clusters.html#create

        Returns
        -------

        """
        METHOD = 'POST'
        API_PATH = 'clusters/create'

        # Check if spark_version supported:
        if not spark_version in self.spark_versions() and not custom_spark_version:
            raise ValueError("'{0}' is not a recognized spark_version. Please see the ".format(spark_version) +
                             "spark_versions() method for available Spark Versions. ")

        available_vms = self.list_available_node_type_names()
        driver_vm_id = kwargs.get('driver_node_type_id')

        # Check if node_type available supported:
        if not node_type_id in available_vms or (driver_vm_id and driver_vm_id not in available_vms):
            raise ValueError("'{0}' is not an available VM type. Please see the ".format(node_type_id) +
                             "list_available_node_type_names() method for available node types")

        cluster_config = {'cluster_name': cluster_name,
                          'spark_version': spark_version,
                          'node_type_id': node_type_id}

        # If python_version is set to Python 3, then overwrite the PYSPARK_PYTHON environment variable
        if python_version == 3:
            if kwargs.get('spark_env_vars'):
                kwargs['spark_env_vars']['PYSPARK_PYTHON'] = '/databricks/python3/bin/python3'
            else:
                kwargs['spark_env_vars'] = {'PYSPARK_PYTHON': '/databricks/python3/bin/python3'}

        # Set default value of autotermination minutes - this defaults to 60 minutes.
        if autotermination_minutes:
            kwargs['autotermination_minutes'] = autotermination_minutes

        # Specify the size of the cluster
        if type(num_workers) == 'dict':
            cluster_config['autoscale'] = num_workers
        else:
            cluster_config['num_workers'] = int(num_workers)

        # Merge kwargs and cluster_config
        cluster_config = dict_update(kwargs, cluster_config)

        resp = self._rest_call[METHOD](API_PATH, data=cluster_config)

        if resp.status_code == 200:
            return resp.json()['cluster_id']

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def edit(self):
        METHOD = 'POST'
        raise NotImplementedError

    def start(self, cluster_name=None, cluster_id=None):
        """
        Start the cluster

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be started.

        Returns
        -------
            The cluster ID of the started cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found
        """
        METHOD = 'POST'
        API_PATH = 'clusters/start'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def restart(self, cluster_name=None, cluster_id=None):
        """
        Restart the cluster

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be restarted.

        Returns
        -------
            The cluster ID of the restarted cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found
        """
        METHOD = 'POST'
        API_PATH = 'clusters/restart'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def resize(self):
        METHOD = 'POST'
        raise NotImplementedError

    def __send_cluster_id_to_endpoint(self, method, api_path, cluster_name, cluster_id):
        """
        Private method to post cluster id only to a given endpoint

        Parameters
        ----------
        method : str
            HTTP POST or GET method
        api_path : str
            API path that post request is sent to

        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be terminated.

        Returns
        -------
            The cluster ID of a stopped cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found

        Returns
        -------

        """
        if not (cluster_name or cluster_id):
            raise ValueError("Either cluster_id or cluster_name must be specified")

        if cluster_name and not cluster_id:
            try:
                cluster_id = self.get_cluster_id(cluster_name)
            except ResourceDoesNotExist:
                raise ResourceDoesNotExist("No cluster named '{0}' was found".format(cluster_name))

        data = {"cluster_id": cluster_id}

        resp = self._rest_call[method](api_path, data=data)

        if resp.status_code == 200 and method == 'GET':
            return resp.json()

        elif resp.status_code == 200:
            return cluster_id

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def terminate(self, cluster_name=None, cluster_id=None):
        """
        Terminate a cluster given a cluster name or cluster ID

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be terminated.

        Returns
        -------
            The cluster ID of a stopped cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found

        """
        METHOD = 'POST'
        API_PATH = 'clusters/delete'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def permanent_delete(self, cluster_name=None, cluster_id=None):
        """
        Permanently deletes a cluster given a cluster name or cluster ID. These clusters are removed from
        cluster list and can no longer be restarted, etc.

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be terminated.

        Returns
        -------
            The cluster ID of a stopped cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found

        """
        METHOD = 'POST'
        API_PATH = 'clusters/permanent-delete'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def get_cluster_id(self, cluster_name):
        """
        Given a cluster name, get the cluster ID for that cluster

        Parameters
        ----------
        cluster_name : str
            Display name of the cluster

        Returns
        -------
            Sorts clusters by last_activity_time then returns ID of the first cluster found with matching cluster_name
            where state is RUNNING. If no clusters with that name are running, returns the first cluster

        Raises
        ------
        ResourceDoesNotExist
            When no matching cluster name and cluster state are found
        """
        ClusterInfo = collections.namedtuple('ClusterInfo', ['id', 'state', 'start_time'])

        # Get all clusters
        clusters = self.list()

        found_clusters = [
            ClusterInfo(id=cluster['cluster_id'], state=cluster['state'], start_time=cluster['start_time'])
            for cluster in clusters if cluster['cluster_name'] == cluster_name]

        if len(found_clusters) == 0:
            raise ResourceDoesNotExist("No cluster named '{0}' was found".format(cluster_name))

        found_clusters = sorted(found_clusters, key=lambda cluster: cluster.start_time)
        running_clusters = list(filter(lambda cluster: cluster.state == 'RUNNING', found_clusters))

        if len(running_clusters) >= 1:
            return running_clusters[0].id
        else:
            return found_clusters[0].id

    def get(self, cluster_name=None, cluster_id=None):
        """
        Get details of the cluster

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be retrieved.

        Returns
        -------
            The details of the cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found
        """
        METHOD = 'GET'
        API_PATH = 'clusters/get'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def pin(self, cluster_name=None, cluster_id=None):
        """
        Pin the cluster to the cluster list

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be pinned.

        Returns
        -------
            The cluster ID of the pinned cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found
        """
        METHOD = 'POST'
        API_PATH = 'clusters/pin'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def unpin(self, cluster_name=None, cluster_id=None):
        """
        Pin the cluster to the cluster list

        Either cluster_name or cluster_id are required. If both are specified cluster_name is ignored.

        Parameters
        ----------
        cluster_name : str, optional
            The name of the cluster.

        cluster_id : str, optional
            The id of the cluster to be unpinned.

        Returns
        -------
            The cluster ID of the unpinned cluster

        Raises
        ------
        ValueError
            When neither cluster_name or cluster_id are passed
        ResourceDoesNotExist
            When a cluster with the given name or id aren't found
        """
        METHOD = 'POST'
        API_PATH = 'clusters/unpin'
        return self.__send_cluster_id_to_endpoint(method=METHOD,
                                                  api_path=API_PATH,
                                                  cluster_name=cluster_name,
                                                  cluster_id=cluster_id)

    def list(self):
        METHOD = 'GET'
        API_PATH = 'clusters/list'

        resp = self._rest_call[METHOD](API_PATH)

        if resp.status_code == 200:
            return resp.json().get('clusters')

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))


    def list_node_types(self):
        """
        List details on all possible node types for Databricks.

        Not all node types will be available for the given subscription.

        :return: List object with information (dict) of all possible node
        """
        METHOD = 'GET'
        API_PATH = 'clusters/list-node-types'

        resp = self._rest_call[METHOD](API_PATH)

        if resp.status_code == 200:
            return resp.json()['node_types']

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def list_available_node_type_names(self):
        """
        Get a list of the node_type_ids available in the current subscription.
        Filter out
        :return:
        """
        nodes = self.list_node_types()
        return [node['node_type_id'] for node in nodes if
                node['node_info'].get('status') is None and node['node_info']['available_core_quota'] >= node[
                    'num_cores']]

    def spark_versions(self):
        METHOD = 'GET'
        API_PATH = 'clusters/spark-versions'

        resp = self._rest_call[METHOD](API_PATH)
        if resp.status_code == 200:
            return {item['key']: item['name'] for item in resp.json()['versions']}

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def events(self):
        METHOD = 'POST'

        raise NotImplementedError
