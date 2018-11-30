# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import base64
import os
from collections import namedtuple

from azure_databricks_api.__base import RESTBase
from azure_databricks_api.__utils import file_content_to_b64
from azure_databricks_api.exceptions import *

MB_BYTES = 1048576

FileInfo = namedtuple("FileInfo", ['path', 'is_dir', 'file_size'])
FileReadInfo = namedtuple("FileReadInfo", ['bytes_read', 'data'])


class DbfsAPI(RESTBase):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_block(self, handle, data_block):
        """
        Adds a block of data to the specified handle

        Parameters
        ----------
        handle : int
            The handle on an open stream. This field is required.

        data_block : bytes
            The base64-encoded data to append to the stream. This has a limit of 1 MB. This field is required.

        Returns
        -------
        handle if successful

        Raises
        ------
        MaxBlockSizeExceeded:
            If the block of data sent is greater that 1 MB

        ResourceDoesNotExist:
            If the handle does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/dbfs/add-block'

        data = {"handle": handle,
                "data": data_block.decode('utf-8')}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return handle

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def close(self, handle):
        """
        Closes the specified handle

        Parameters
        ----------
        handle : int
            The handle on an open stream. This field is required.

        Returns
        -------
        handle if successful

        Raises
        ------
        ResourceDoesNotExist:
            If the handle does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/dbfs/close'

        data = {"handle": handle}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return handle

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def create(self, path, overwrite=False):
        """
        Opens a new DBFS handle

        Parameters
        ----------
        path : str
            The path of the new file. The path should be the absolute DBFS path (e.g. “/mnt/foo.txt”). This field is required.

        overwrite : bool optional
            The flag that specifies whether to overwrite existing file/files.

        Returns
        -------
        handle if successful

        Raises
        ------
        MaxBlockSizeExceeded:
            If blocksize sent is greater that 1 MB

        ResourceDoesNotExist:
            If the handle does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/dbfs/create'

        data = {"path": path,
                "overwrite": overwrite}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return resp.json().get('handle')

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def delete(self, path, recursive=False, not_exists_ok=False):
        """

        Parameters
        ----------
        path : str
            The path of the file or directory to delete. The path should be the absolute DBFS path (e.g. “/mnt/foo/”).
            This field is required.
        recursive : bool
            Whether or not to recursively delete the directory’s contents. Deleting empty directories can be done
            without providing the recursive flag.
        not_exists_ok : bool
            Suppress any exceptions caused by trying to delete a file that does not exist.

        Returns
        -------
        path if successful

        Raises
        ------
        IOError:
            If the path is a non-empty directory and recursive is set to false or on other similar errors

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/dbfs/delete'

        data = {"path": path,
                "recursive": recursive}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") == "RESOURCE_DOES_NOT_EXIST" and not_exists_ok:
                return path
            elif resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def get_status(self, path):
        """
        Gets the file information of a file or directory.

        Parameters
        ----------
        path : str
            The path of the file or directory. The path should be the absolute DBFS path (e.g. “/mnt/foo/”).
            This field is required.


        Returns
        -------
        FileInfo named tuple with path, is_dir and file_size

        Raises
        ------
        ResourceDoesNotExist:
            If the file or directory does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above

        """
        METHOD = 'GET'
        API_PATH = '/dbfs/get-status'

        data = {"path": path}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return FileInfo(**resp.json())

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
        """
        Lists the contents of a directory, or details of the file.

        Parameters
        ----------
        path : str
            The path of the file or directory. The path should be the absolute DBFS path (e.g. “/mnt/foo/”).
            This field is required.

        Returns
        -------
        Array of FileInfo named tuples (with path, is_dir and file_size)

        Raises
        ------
        ResourceDoesNotExist:
            If the file or directory does not exist

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'GET'
        API_PATH = '/dbfs/list'

        data = {"path": path}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return [FileInfo(**file) for file in resp.json().get('files')]

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def mkdirs(self, path):
        """
        Creates the given directory and necessary parent directories if they do not exist.

        Note: that if this operation fails it may have succeeded in creating some of the necessary parent directories.

        Parameters
        ----------
        path : str
            The path of the new directory. The path should be the absolute DBFS path (e.g. “/mnt/foo/”). This field is required.

        Returns
        -------
        path if successful

        Raises
        ------
        ResourceAlreadyExists:
            If there exists a file (not a directory) at any prefix of the input path

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured above
        """
        METHOD = 'POST'
        API_PATH = '/dbfs/mkdirs'

        data = {"path": path}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def move(self, source_path, destination_path):
        """
        Move a file or directory from one location to another location within DBFS. If the given source path is a
        directory, this will always recursively move all files.

        Parameters
        ----------
        source_path : str
            The source path of the file or directory. The path should be the absolute DBFS path (e.g. “/mnt/foo/”).
            This field is required.
        destination_path : str
            The destination path of the file or directory. The path should be the absolute DBFS path (e.g. “/mnt/bar/”).
            This field is required.

        Returns
        -------
        destination_path if successful

        Raises
        ------
        ResourceDoesNotExist:
             If the source file does not exist

        ResourceAlreadyExists:
            If there already exists a file in the destination path

        AuthorizationError:
            If the services returns a 403 status code

        APIError:
            If the status code returned by the service is anything except 200 and is not captured abov

        """
        METHOD = 'POST'
        API_PATH = '/dbfs/move'

        data = {"source_path": source_path,
                "destination_path": destination_path}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return destination_path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def __put(self, path, data, overwrite=False):
        METHOD = 'POST'
        API_PATH = '/dbfs/put'

        payload = {"path": path,
                   "contents": data,
                   "overwrite": overwrite}

        resp = self._rest_call[METHOD](API_PATH, data=payload)

        if resp.status_code == 200:
            return path

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def __read(self, path, offset, length=MB_BYTES):
        METHOD = 'GET'
        API_PATH = '/dbfs/read'

        data = {"path": path,
                "offset": offset,
                "length": length}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return FileReadInfo(**resp.json())

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def download_file(self, local_path, dbfs_path, overwrite=False, chunk_size=MB_BYTES):
        """
        Downloads a file from DBFS and saves to a local path

        Parameters
        ----------
        local_path : str
            The local path where the file should be saved
        dbfs_path : str
            The DBFS path to be downloaded
        overwrite : bool
            If a file exists at the destination, overwrite the file
        chunk_size : int
            The size (in bytes) to be read during each call of the API

        Returns
        -------
        local path if successful

        """

        if os.path.exists(local_path) and not overwrite:
            raise FileExistsError("The local path {0} already exists.".format(local_path))

        # Get the file info from the get_status endpoint
        file_info = self.get_status(dbfs_path)

        if file_info.is_dir:
            raise NotImplementedError("Downloading an entire DBFS directory is not currently supported.")

        with open(local_path, 'wb') as file_obj:
            downloaded_size = 0

            # Loop until we've downloaded the whole file
            while downloaded_size < file_info.file_size:
                chunk = self.__read(path=dbfs_path, offset=downloaded_size, length=chunk_size)

                file_obj.write(base64.b64decode(chunk.data))
                downloaded_size += chunk.bytes_read

        return local_path

    def upload_file_by_path(self, file_path, dbfs_path, overwrite=False, chunk_size=MB_BYTES):
        """
        Uploads a file to DBFS and from a local path

        Parameters
        ----------
        local_path : str
            The local path where the file should be saved
        dbfs_path : str
            The DBFS path to be downloaded
        overwrite : bool
            If a file exists at the destination, overwrite the file
        chunk_size : int
            The size (in bytes) to be read during each call of the API

        Returns
        -------
        local path if successful

        """
        file_size = os.path.getsize(file_path)

        if file_size <= MB_BYTES:
            return self.__put(dbfs_path, file_content_to_b64(file_path), overwrite=overwrite)

        with open(file_path, 'rb') as file_obj:
            stream_handle = self.create(dbfs_path, overwrite)

            for chunk in self.__get_chunks(file_size, chunk_size):
                data = file_obj.read(chunk)
                self.add_block(stream_handle, base64.b64encode(data))

            self.close(handle=stream_handle)

        return dbfs_path

    @staticmethod
    def __get_chunks(file_size, chunk_size=MB_BYTES):
        chunk_start = 0
        while chunk_start + chunk_size < file_size:
            yield chunk_size
            chunk_start += chunk_size

        final_chunk_size = file_size - chunk_start
        yield final_chunk_size
