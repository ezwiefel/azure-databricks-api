# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import base64
import collections
import io

import requests


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
