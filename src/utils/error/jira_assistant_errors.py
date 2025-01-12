from .base_custom_error import BaseCustomError


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


class JiraMetadataFetchError(JiraManagerError):
    """
    Raised when fetching metadata for a specific issue type fails.
    """

    def __init__(self, message: str = "Failed to fetch metadata for issue type", **metadata):
        super().__init__(message, **metadata)
