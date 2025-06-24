from utils.error.base_custom_error import BaseCustomError


class JiraManagerError(BaseCustomError):
    """
    Base exception class for all Jira Manager related errors.
    """

    pass


class JiraAPIConnectionError(JiraManagerError):
    """
    Raised when there's a failure connecting to the Jira API.
    """

    def __init__(self, message: str = "Failed to connect to the Jira API", **metadata):
        super().__init__(message, **metadata)


class JiraQueryError(JiraManagerError):
    """
    Raised when a JQL query execution fails.
    """

    def __init__(self, message: str = "Error executing JQL query", **metadata):
        super().__init__(message, **metadata)


class JiraIssueCreationError(JiraManagerError):
    """
    Raised when there's an error creating an issue in Jira.
    """

    def __init__(self, message: str = "Failed to create a Jira issue", **metadata):
        super().__init__(message, **metadata)


class JiraComponentFetchError(JiraManagerError):
    """
    Raised when fetching components for a project fails.
    """

    def __init__(self, message: str = "Failed to fetch project components", **metadata):
        super().__init__(message, **metadata)


class JiraComponentCreationError(JiraManagerError):
    """
    Raised when creating a component fails.
    """

    def __init__(self, message: str = "Failed to create component", **metadata):
        super().__init__(message, **metadata)


class JiraComponentDeletionError(JiraManagerError):
    """
    Raised when deleting a component fails.
    """

    def __init__(self, message: str = "Failed to delete component", **metadata):
        super().__init__(message, **metadata)


class JiraIssueComponentUpdateError(JiraManagerError):
    """
    Raised when updating issue components fails.
    """

    def __init__(self, message: str = "Failed to update issue components", **metadata):
        super().__init__(message, **metadata)


class JiraMetadataFetchError(JiraManagerError):
    """
    Raised when fetching metadata for a specific issue type fails.
    """

    def __init__(self, message: str = "Failed to fetch metadata for issue type", **metadata):
        super().__init__(message, **metadata)


class JiraApiClientError(BaseCustomError):
    """
    Base exception class for JiraApiClient.
    """

    pass


class JiraApiRequestError(JiraApiClientError):
    """
    Raised for errors during API requests.
    """

    def __init__(self, message: str, endpoint: str, payload=None, params=None, status_code=None):
        super().__init__(
            message,
            endpoint=endpoint,
            payload=payload,
            params=params,
            status_code=status_code,
        )
