# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import collections


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
