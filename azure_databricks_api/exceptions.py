"""
Custom Exceptions based on the Databricks Error Codes
"""
from requests import HTTPError


class ResourceAlreadyExists(ValueError):
    """Raise this exception if a path exists and you are trying to create it again"""

class ResourceDoesNotExist(ValueError):
    """Raise this exception if a resource doesn't exist where you expect it to"""

class APIError(Exception):
    """General purpose error to catch errors returned from Databricks"""

class UnknownFormat(AttributeError):
    """Specified format type doesn't exist"""

class MaxNotebookSizeExceeded(HTTPError):
    """Notebook to be downloaded is over 10MB"""

class AuthorizationError(HTTPError):
    """User is not authorized or token is incorrect"""