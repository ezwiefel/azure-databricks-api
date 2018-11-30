# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT

import collections

from azure_databricks_api.__base import RESTBase
from azure_databricks_api.exceptions import APIError, AuthorizationError, ERROR_CODES


class GroupsAPI(RESTBase):

    def __init__(self, **kwargs):
        super(GroupsAPI, self).__init__(**kwargs)

    def __prep_group_or_user(self, group_name=None, user_name=None):
        """
        Prepare a Python object containing the user_name or group_name key

        Parameters
        ----------
        group_name : str, optional
            The name of a group

        user_name : str, optional
            The name of a user

        Returns
        -------
        dict
            A dictionary containing the group_name or user_name to be passed along to the next object

        Raises
        ------
        ValueError
            If both group_name and user_name are defined or if neither group_name or user_name are defined
        """

        # Check if both variable are passed OR neither variables are passed
        if group_name and user_name:
            raise ValueError("Both group_name and user_name were defined. Only of these values can be defined.")
        elif not group_name and not user_name:
            raise ValueError("Neither group_name or user_name were defined. One of these values are required.")

        if group_name:
            return {'group_name': group_name}, group_name
        else:
            return {'user_name': user_name}, user_name

    def add_member(self, parent_group, group_name=None, user_name=None):
        """
        Adds a new member (either user or group) to a given parent group

        Parameters
        ----------
        parent_group : str
            The group to which the new user or group should be added
        group_name : str, optional
            A group to be added to parent group
        user_name : str, optional
            A user to be added to parent group

        Returns
        -------
        str : The group name or user name added

        Raises
        ------
        ValueError
            If both group_name and user_name are defined or if neither group_name or user_name are defined

        """
        METHOD = 'POST'
        API_PATH = '/groups/add-member'

        # Process group_name and user_name and add parent name to resulting dict
        data, target_name = self.__prep_group_or_user(group_name=group_name, user_name=user_name)
        data['parent_name'] = parent_group

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return target_name

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def create(self, group_name):
        """
        Creates a new group

        Parameters
        ----------
        group_name : str
            A group to be created

        Returns
        -------
        dict : A Python representation of the JSON returned by the API

        """
        METHOD = 'POST'
        API_PATH = '/groups/create'

        data = {'group_name': group_name}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)
        if resp.status_code == 200:
            return resp.json()

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def list_members(self, group_name):
        """
        Lists members of a given group

        Parameters
        ----------
        group_name : str
            A group for which members should be listed

        Returns
        -------
        list : A list of Python dict objects (specifying user_name or group_name

        """
        METHOD = 'GET'
        API_PATH = '/groups/list-members'

        data = {'group_name': group_name}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return resp.json().get('members')

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def list(self):
        """
        Lists all groups in the workspace

        Returns
        -------
        list : A list of of group_names

        """
        METHOD = 'GET'
        API_PATH = '/groups/list'

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH)

        if resp.status_code == 200:
            return resp.json().get('group_names')

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def list_parents(self, group_name=None, user_name=None):
        """
        Lists all groups of a given user or group

        Parameters
        ----------
        group_name : str, optional
            The name of a group

        user_name : str, optional
            The name of a user

        Returns
        -------
        list : A list of of group_names

        """
        METHOD = 'GET'
        API_PATH = '/groups/list-parents'

        # Process group_name and user_name
        data, target_name = self.__prep_group_or_user(group_name=group_name, user_name=user_name)

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return resp.json().get('group_names')

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def remove_member(self, parent_group, remove_group=None, remove_user=None):
        """
        Removes a member (either user or group) from a given parent group

        Parameters
        ----------
        parent_group : str
            The group from which the user or group should be removed
        group_name : str, optional
            A group to be removed from parent group
        user_name : str, optional
            A user to be removed from parent group

        Returns
        -------
        str : The group name or user name added

        Raises
        ------
        ValueError
            If both group_name and user_name are defined or if neither group_name or user_name are defined

        """
        METHOD = 'POST'
        API_PATH = '/groups/remove-member'

        # Process group_name and user_name and add parent name to resulting dict
        data, target_name = self.__prep_group_or_user(group_name=remove_group, user_name=remove_user)
        data['parent_name'] = parent_group

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return target_name

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))

    def delete(self, group_name):
        """
        Deletes a group

        Parameters
        ----------
        group_name : str
            A group to be deleted

        Returns
        -------
        str : The name of the removed group

        """
        METHOD = 'POST'
        API_PATH = '/groups/delete'

        data = {'group_name': group_name}

        # Make REST call
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return group_name

        elif resp.status_code == 403:
            raise AuthorizationError("User is not authorized or token is incorrect.")

        else:
            if resp.json().get("error_code") in ERROR_CODES:
                raise ERROR_CODES[resp.json().get('error_code')](resp.json().get('message'))
            else:
                raise APIError("Response code {0}: {1} {2}".format(resp.status_code,
                                                                   resp.json().get('error_code'),
                                                                   resp.json().get('message')))
