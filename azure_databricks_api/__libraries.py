# Copyright (c) 2018 Microsoft
#
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from azure_databricks_api.__base import RESTBase
from azure_databricks_api.exceptions import ResourceDoesNotExist, LibraryNotFound, LibraryInstallFailed, APIError, AuthorizationError, ERROR_CODES
import time


class LibrariesAPI(RESTBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def all_cluster_statuses(self):
        """
        Returns library status for all clusters.

        Parameters
        ----------
        Returns
        -------
            A json string containing the libraries installed on all clusters

            Format here : https://docs.azuredatabricks.net/dev-tools/api/latest/libraries.html#all-cluster-statuses

        """
        METHOD = 'GET'
        API_PATH = '/libraries/all-cluster-statuses'

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH)

        if resp.status_code == 200:
            return resp.json()

        elif resp.status_code == 403:
            raise AuthorizationError(
                "User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](
                    resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def cluster_status(self, cluster_id):
        """
        Returns library status for a specific cluster.

        Parameters
        ----------
        cluster_id : str
            The cluster ID to query

        Returns
        -------
            A json string containing the libraries installed on this cluster_id
            Format here : https://docs.azuredatabricks.net/dev-tools/api/latest/libraries.html#cluster-status
        """
        METHOD = 'GET'
        API_PATH = '/libraries/cluster-status'

        data = {'cluster_id': cluster_id}
        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return resp.json()

        elif resp.status_code == 403:
            raise AuthorizationError(
                "User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](
                    resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def get_library_details(self, cluster_id, library_name, library_type=None):
        """
        Returns the details of a specific library on a specific clusters.

        Parameters
        ----------
        cluster_id : str
            The cluster ID to query

        library_name : str
            The name of the libary being installed.

            pip name, maven coordinates, etc.

        library_type : str
            The type of library - maven, pypi, cran, etc.

        Returns
        -------
            The details of a specific library on a specific cluster.

            See https://docs.azuredatabricks.net/dev-tools/api/latest/libraries.html#libraryfullstatus
        """
        all_packages = self.cluster_status(cluster_id)

        for library in all_packages['library_statuses']:
            lib_details = list(library['library'].items())[0]

            if library_type is not None:
                if not lib_details[0] == library_type:
                    continue

            # Get the current library name - depending on library type
            # e.g. pypi libraries are named "package", maven are named "coordinates"
            if lib_details[0] in ["jar", "egg", "whl"]:
                current_lib_name = lib_details[1]
            elif lib_details[0] == "maven":
                current_lib_name = lib_details[1]['coordinates']
            else:
                current_lib_name = lib_details[1]["package"]

            # If the library name is the one we're searching for, return that name
            if library_name == current_lib_name:
                return library

        lib_string = "'{0}' library ".format(
            library_type) if library_type else ""
        lib_string = lib_string + "'{0}'".format(library_name)

        raise LibraryNotFound(
            "{0} is not found on cluster '{1}'".format(lib_string, cluster_id))

    def wait_for_install_complete(self, cluster_id, library_name, library_type=None, timeout=120):
        """
        Blocks the code until the library status is "INSTALLED". If library status == "FAILED" raises exception

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        library_name : str
            The name of the libary being installed.

            PIP name, Maven Coordinates, etc.

        library_type : str
            The type of library - maven, pypi, cran, etc.

        timeout : int
            The time in seconds to wait for the installation to complete
        """
        iteration = 0
        start_time = time.time()
        status = None

        while status != 'INSTALLED':
            lib_details = self.get_library_details(
                cluster_id, library_name, library_type)
            status = lib_details['status']
            if status == 'INSTALLED':
                break
            elif status == 'FAILED':
                raise LibraryInstallFailed(
                    "'{0}' failed to install. Response: {1}".format(library_name, lib_details))

            if time.time() - start_time >= timeout:
                raise TimeoutError("The status check timed out")

            # Exponential backoff in tenths of a second
            sleep_time = (2**iteration) / 10
            time.sleep(sleep_time)

        return lib_details

    def install(self, cluster_id, libraries, wait_for_completion=False, timeout=120):
        """
        Installs new libraries on the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        libraries : array of libraries
            see https://docs.azuredatabricks.net/dev-tools/api/latest/libraries.html#install

        Returns
        -------
        Cluster library status for given cluster
        """
        METHOD = 'POST'
        API_PATH = '/libraries/install'

        # create payload to add librairies
        data = {'cluster_id': cluster_id,
                'libraries': libraries}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return self.cluster_status(cluster_id)

        elif resp.status_code == 403:
            raise AuthorizationError(
                "User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](
                    resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def install_pypi(self, cluster_id, package, repo=None, wait_for_completion=False, timeout=120):
        """
        Installs a new Pypi library on the cluster.

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        library : str
            The name of the PyPI package to install. An optional exact version specification is also supported. Examples: simplejson and simplejson==3.8.0. This field is required.

        repo : str - default None
            The repository where the package can be found. If not specified, the default pip index is used.

        Returns
        -------
        """

        library = {'pypi': {"package": package}}

        if repo is not None:
            library['pypi']['repo'] = repo

        resp = self.install(cluster_id, [library])

        if wait_for_completion:
            resp = self.wait_for_install_complete(
                cluster_id, package, 'pypi', timeout)

        return resp

    def install_cran(self, cluster_id, package, repo=None, wait_for_completion=False, timeout=120):
        """
        Installs a new CRAN library on the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        library : str
            The name of the CRAN package to install. This field is required.

        repo : str - default None
            The repository where the package can be found. If not specified, the default CRAN repo is used.

        Returns
        -------
        """
        library = {'cran': {"package": package}}

        if repo is not None:
            library['cran']['repo'] = repo

        resp = self.install(cluster_id, [library])

        if wait_for_completion:
            resp = self.wait_for_install_complete(
                cluster_id, package, 'cran', timeout)

        return resp

    def install_maven(self, cluster_id, coordinates, repo=None, exclusions=None, wait_for_completion=False, timeout=120):
        """
        Installs a new CRAN library on the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        coordinates : str
            Gradle-style Maven coordinates. For example: org.jsoup:jsoup:1.7.2. This field is required.

        repo : str - default None
            Maven repo to install the Maven package from. If omitted, both Maven Central Repository and Spark Packages are searched.

        exclustion : list - default None
            List of dependences to exclude. For example: ["slf4j:slf4j", "*:hadoop-client"].

        Returns
        -------
        Cluster library status for given cluster
        """
        library = {'maven': {"coordinates": coordinates}}

        if repo is not None:
            library['maven']['repo'] = repo

        if exclusions is not None:
            if type(exclusions) != list:
                exclusions = [exclusions]

            library['maven']['exclusions'] = exclusions

        resp = self.install(cluster_id, [library])

        if wait_for_completion:
            resp = self.wait_for_install_complete(
                cluster_id, coordinates, 'maven', timeout)

        return resp

    def uninstall(self, cluster_id, libraries):
        """
        Uninstalls a library from the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to uninstall libraries

        libraries : array of libraries
            see https://docs.azuredatabricks.net/dev-tools/api/latest/libraries.html#uninstall

        Returns
        -------
        """

        METHOD = 'POST'
        API_PATH = '/libraries/uninstall'

        # create payload to add librairies
        data = {'cluster_id': cluster_id,
                'libraries': libraries}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return self.cluster_status(cluster_id)

        elif resp.status_code == 403:
            raise AuthorizationError(
                "User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](
                    resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def uninstall_pypi(self, cluster_id, package):
        """
        Uninstalls a Pypi library on the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        library : str
            The name of the PyPI package to uninstall.

        Returns
        -------
        """
        library = {'pypi': {"package": package}}

        return self.uninstall(cluster_id, [library])

    def uninstall_cran(self, cluster_id, package):
        """
        Uninstalls a CRAN library on the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        library : str
            The name of the CRAN package to uninstall.

        Returns
        -------
        """
        library = {'cran': {"package": package}}

        return self.uninstall(cluster_id, [library])

    def uninstall_maven(self, cluster_id, coordinates):
        """
        Installs a new CRAN library on the cluster

        This is an async call. You can check the status of library installation using the 'cluster_status' method.

        Parameters
        ----------
        cluster_id : str
            The display name of the cluster on which to install libraries

        coordinates : str
            Gradle-style Maven coordinates. For example: org.jsoup:jsoup:1.7.2. This field is required.

        Returns
        -------
        Cluster library status for given cluster
        """
        library = {'maven': {"coordinates": coordinates}}

        return self.uninstall(cluster_id, [library])
