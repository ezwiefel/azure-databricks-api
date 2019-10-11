"""
Custom Exceptions based on the Databricks Error Codes
"""
from requests import HTTPError


class ResourceAlreadyExists(ValueError):
    """Raise this exception if a path exists and you are trying to create it again"""


class ResourceDoesNotExist(ValueError):
    """Raise this exception if a resource doesn't exist where you expect it to"""

class LibraryNotFound(ValueError):
    """Raise this exception if an expected library is not found on the cluster"""

class LibraryInstallFailed(ValueError):
    """Raise this exception if a library installation fails"""

class APIError(Exception):
    """General purpose error to catch errors returned from Databricks"""


class UnknownFormat(AttributeError):
    """Specified format type doesn't exist"""


class MaxNotebookSizeExceeded(HTTPError):
    """Notebook to be downloaded is over 10MB"""


class AuthorizationError(HTTPError):
    """User is not authorized or token is incorrect"""


class MaxBlockSizeExceeded(HTTPError):
    """Block of data exceeds 1 MB"""


class MaxReadSizeExceeded(HTTPError):
    """Read length exceeds 1 MB"""


class IoError(HTTPError):
    """During DBFS operations returned if path is non-empty and recursive is set to false (or similiar errors)"""


class InvalidParameterValue(HTTPError):
    """An invalid parameter was passed (such as a negative byte offset) in DBFS read operations"""


class DirectoryNotEmpty(HTTPError):
    """The directory is not empty"""

class InvalidState(HTTPError):
    """The cluster is in an invalid state for the given request"""

ERROR_CODES = {
    "RESOURCE_DOES_NOT_EXIST": ResourceDoesNotExist,
    "RESOURCE_ALREADY_EXISTS": ResourceAlreadyExists,
    "MAX_NOTEBOOK_SIZE_EXCEEDED": MaxNotebookSizeExceeded,
    "MAX_BLOCK_SIZE_EXCEEDED": MaxBlockSizeExceeded,
    "MAX_READ_SIZE_EXCEEDED": MaxReadSizeExceeded,
    "IO_ERROR": IoError,
    "INVALID_PARAMETER_VALUE": InvalidParameterValue,
    "DIRECTORY_NOT_EMPTY": DirectoryNotEmpty,
    "INVALID_STATE": InvalidState
}
