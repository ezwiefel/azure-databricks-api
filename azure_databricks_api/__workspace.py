# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

from collections import namedtuple

from azure_databricks_api.__base import RESTBase
from azure_databricks_api.__utils import url_content_to_b64, file_content_to_b64
from azure_databricks_api.exceptions import APIError, UnknownFormat, \
    AuthorizationError, ERROR_CODES

WorkspaceObjectInfo = namedtuple("ObjectInfo", ['object_type', 'path', 'language'])
WorkspaceObjectInfo.__new__.__defaults__ = (None,)

EXPORT_FORMATS = ['SOURCE', 'JUPYTER', 'DBC', 'HTML']
LANGUAGES = ['PYTHON', 'R', 'SQL', 'SCALA']


class WorkspaceAPI(RESTBase):

    def __init__(self, **kwargs):
        super(WorkspaceAPI, self).__init__(**kwargs)

    def delete(self, path, recursive=False, not_exists_ok=False):
        """
        Deletes the path in the given workspace.

        Parameters
        ----------
        path : str
            The path, in the Databricks workspace, to delete

        recursive : bool, optional
            Recursively delete the given path

        not_exists_ok : bool, optional
            If the given path is not found, avoid raising error

        Returns
        -------
        path if successfully deleted

        Raises
        ------
        ResourceDoesNotExist:
            If not_exists_ok is set to False and the given path does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/workspace/delete'

        data = {'path': path,
                'recursive': recursive}
        resp = self._rest_call[METHOD](API_PATH, data=data)

        # Process response
        if resp.status_code == 200:
            return path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                if resp.json().get("error_code") == "RESOURCE_DOES_NOT_EXIST" and not_exists_ok:
                    return path
                else:
                    raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def export(self, dbx_path, file_path, file_format='DBC'):
        """ Exports the Databricks path to a file on the local PC.

        Parameters
        ----------
        dbx_path : str
            The path, in the Databricks workspace, to export

        file_path : str
            The path, on the local PC, where the file should be created

        file_format: str, optional
            The format of the file to be saved. Defaults to DBC. Must be in SOURCT

        Returns
        -------
        file_path if successful

        Raises
        ------
        ResourceDoesNotExist:
            If the given Databricks path does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'GET'
        API_PATH = '/workspace/export'

        if file_format.upper() not in EXPORT_FORMATS:
            raise UnknownFormat('{0} is not a supported format type. Please use DBC, SOURCE, HTML, or JUPYTER')

        data = {'path': dbx_path,
                'format': file_format,
                'direct_download': True}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            with open(file_path, 'wb+') as fo:
                fo.write(resp.content)

            return file_path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))
    def get_status(self, path):
        """ Gets the status of a given Databricks path

        Parameters
        ----------
        path : str
            The path, in the Databricks workspace, to get the status of

        Returns
        -------
            WorkspaceObject - details of the item at given path

        Raises
        ------
        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'GET'
        API_PATH = '/workspace/get-status'

        data = {'path': path}
        resp = self._rest_call[METHOD](API_PATH, data=data)

        # Process response
        if resp.status_code == 200:
            return WorkspaceObjectInfo(**resp.json())

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def import_file(self, dbx_path, file_format, language="", overwrite=False, url=None, filepath=None):
        """ Imports a file to the Databricks workspace from a given URL or file path

        Parameters
        ----------
        dbx_path : str
            The path, in the Databricks workspace, where the object should be created

        file_format : str
            The format of the file imported. Options are SOURCE, HTML, JUPYTER, DBC

        language : str, optional
            Required if file_format is set to SOURCE

            The computer language that the source code is written in. Options are SCALA, PYTHON, SQL or R

        overwrite : bool, optional
            Overwrite the Databricks path (not currently supported for DBC)

        url : str, optional
            The url for the file to be imported. Often this is a Github raw URL.

        filepath : str, optional
            The path on the local PC of the file to be uploaded

        Returns
        -------
        dbx_path if successful

        Raises
        ------
        AttributeError:
            If the requirements for attributes are not met

        MaxNotebookSizeExceeded:
            If imported file size is greater than 10 MB.

        ResourceAlreadyExists:
            If overwrite is set to false and there is already an object at the given dbx_path

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/workspace/import'

        # url XOR filepath defined
        if not (url or filepath):
            raise AttributeError("Must pass either URL or filepath to Workspace Import")
        elif file_format.upper() == 'SOURCE' and language.upper() not in LANGUAGES:
            raise AttributeError("If file_format=SOURCE, language must be Scala, Jupyter, Python or R")
        elif file_format.upper() not in EXPORT_FORMATS:
            raise AttributeError("File format must be SOURCE, DBC, JUPYTER or HTML")

        if url:
            content = url_content_to_b64(url)
        else:
            content = file_content_to_b64(filepath)

        data = {
            "content": content.decode('utf-8'),
            "format": file_format.upper(),
            "overwrite": overwrite,
            "path": dbx_path
        }

        if file_format.upper() == 'SOURCE':
            data['language'] = language.upper()

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return dbx_path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def list(self, path):
        """Lists the contents of the given director

        Parameters
        ----------
        path : str
            The path, in the Databricks workspace, of which, the contents should be listed

        Returns
        -------
        List of WorkspaceObjectgs

        Raises
        ------
        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'GET'
        API_PATH = '/workspace/list'

        data = {'path': path}
        resp = self._rest_call[METHOD](API_PATH, data=data)

        # Process response
        if resp.status_code == 200:
            if resp.json().get('objects'):
                return [WorkspaceObjectInfo(**obj) for obj in resp.json().get('objects')]
            else:
                return []

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))

    def mkdirs(self, path, exists_ok=False):
        """
        Creates the given directory and necessary parent directories if they do not exist.

        If there exists an object (not a directory) at any prefix of the input path, this call raises an
        error RESOURCE_ALREADY_EXISTS. Note that if this operation fails it may have succeeded in creating
        some of the necessary parent directories.

        Parameters
        ----------
        path : str
            The path, in the Databricks workspace, where a directory should be made

        exists_ok : bool, optional
            Supress an error a resource already exists at the given endpoint

        Returns
        -------
        path : str
            The path that was created

        Raises
        ------
        ResourceAlreadyExists
            If you are trying to create a path that already exists and the exists_ok flag is false.
        APIError
            If the Databricks API returned an error
        """
        METHOD = 'POST'
        API_PATH = '/workspace/mkdirs'

        data = {'path': path}
        resp = self._rest_call[METHOD](API_PATH, data=data)

        # Process response
        if resp.status_code == 200:
            return path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                if resp.json().get("error_code") == "RESOURCE_ALREADY_EXISTS" and exists_ok:
                    return path
                else:
                    raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                               resp.json().get('error_code'),
                                                               resp.json().get('message')))