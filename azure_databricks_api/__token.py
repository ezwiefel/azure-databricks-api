# Copyright (c) 2018 Microsoft
# 
# This software is released under the MIT License.
# https://opensource.org/licenses/MIT
import collections

from azure_databricks_api.__base import RESTBase


TokenInfo = collections.namedtuple('TokenInfo', ['token_id', 'creation_time', 'expiry_time', 'comment'])

class TokensAPI(RESTBase):

    def __init__(self, **kwargs):
        super(TokensAPI, self).__init__(**kwargs)

    def create(self, comment, lifetime_seconds=7776000):
        """
        Creates a new personal access token

        Parameters
        ----------
        comment : str
            The comment to be added for the token to be created
        lifetime_seconds : int, optional, default=7776000 (90 days)
            The lifetime seconds

        Returns
        -------
        Token Value and Info : dict
            Dictionary with token value as 'token_value' key and TokenInfo object as 'token_info'
        """

        METHOD = 'POST'
        API_PATH = '/token/create'

        data = {'lifetime_seconds': lifetime_seconds,
                'comment': comment}

        resp = self._rest_call[METHOD](API_PATH, data=data)

        resp_json = resp.json()

        if resp.status_code == 200:
            return {'token_value': resp_json.get('token_value'),
                    'token_info': TokenInfo(**resp_json.get('token_info'))}
        else:
            return "Error creating token: {0}: {1}".format(resp.json()["error_code"], resp.json()["message"])

    def list(self):
        """
        List all of the tokens in the user's account

        Returns
        -------
        Token Values : list of TokenInfo objects
        """
        METHOD = 'GET'
        API_PATH = '/token/list'

        resp = self._rest_call[METHOD](API_PATH)

        if resp.json().get('token_infos'):
            return [TokenInfo(**token) for token in resp.json().get('token_infos')]
        else:
            return None

    def revoke(self, token_id):
        """
        Deletes a token.

        Parameters
        ----------
        token_id : str
            The ID of the token to be deleted.

        Returns
        -------
        token_id : str
            If the token is deleted.
        """
        METHOD = 'POST'
        API_PATH = '/token/delete'

        data = {'token_id': token_id}
        resp = self._rest_call[METHOD](API_PATH, data=data)

        if resp.status_code == 200:
            return token_id
        else:
            return "Error deleting token: {0}: {1}".format(resp.json()["error_code"], resp.json()["message"])