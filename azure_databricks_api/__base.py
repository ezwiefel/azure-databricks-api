# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import json

import requests


class RESTBase(object):

    def __init__(self, **kwargs):

        self._host = kwargs.pop('host')
        self._api_version = kwargs.pop('api_version')
        self._uri = "{host}/api/{api_version}/".format(host=self._host, api_version=self._api_version)
        self._token = kwargs.pop('token')
        self._headers = {'Authorization': 'Bearer {0}'.format(self._token)}
        self._rest_call = {'GET': self.__get,
                          'POST': self.__post}

    def __get(self, api_endpoint, data=None):
        """
        Send HTTP GET request to REST API endpoint with data as query string

        :param api_endpoint: string : The api endpoint to be called - after version number
        :param data: dict : Data to be passed as query string in url
        :return: Partial function requests.get with URL populated
        """

        # Check that API_endpoint does not start with a '/', if so, remove it.
        # Because self.uri already contains the necessary '/'
        if api_endpoint.startswith('/'):
            api_endpoint = api_endpoint[1:]

        uri = self._uri + api_endpoint
        return requests.get(url=uri, headers=self._headers, json=data)

    def __post(self, api_endpoint, data):
        """
        Send HTTP POST request to REST API endpoint with data as JSON object

        :param api_endpoint:
        :param data:
        :return:
        """

        # Check that API_endpoint does not start with a '/', if so, remove it.
        # Because self.uri already contains the necessary '/'
        if api_endpoint.startswith('/'):
            api_endpoint = api_endpoint[1:]

        data_json = json.dumps(data, ensure_ascii=False)

        uri = self._uri + api_endpoint
        return requests.post(url=uri, headers=self._headers, data=data_json)