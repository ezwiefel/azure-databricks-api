# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import base64
import collections

import requests

from azure_databricks_api.exceptions import APIError, AuthorizationError, ERROR_CODES


def dict_update(source, updates):
    """Update a nested dictionary or similar mapping.

    Modify ``source`` in place.

    From REST_API_Py_Requests_Lib provided by Alex Zeltov
    """
    for key, value in updates.items():
        if isinstance(value, collections.Mapping) and value:
            returned = dict_update(source.get(key, {}), value)
            source[key] = returned
        else:
            source[key] = updates[key]
    return source


def url_content_to_b64(url):
    file_content = requests.get(url)
    encoded_content = base64.b64encode(file_content.content)
    return encoded_content


def file_content_to_b64(file):
    with open(file, 'rb') as file_obj:
        file_content = file_obj.read()

    encoded_content = base64.b64encode(file_content)
    return encoded_content


def choose_exception(response: requests.Response) -> Exception:
    """ Choose the correct error handling message if status is not 200

    Parameters
    ----------
        response: The requests.Response object returned from the API call

    Returns
    -------
        Exception: The appropriate exception to raise
    """
    if response.status_code == 403:  # pragma: no cover
        return_error = AuthorizationError("User is not authorized or token is incorrect.")

    else:  # pragma: no cover
        if response.json().get("error_code") in ERROR_CODES:
            return_error = ERROR_CODES[response.json().get('error_code')](response.json().get('message'))
        else:
            return_error = APIError("Response code {0}: {1} {2}".format(response.status_code,
                                                                        response.json().get('error_code'),
                                                                        response.json().get('message')))
    return return_error
