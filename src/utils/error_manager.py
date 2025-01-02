import requests
import os

from src.log_config import log_manager

logger = log_manager.get_logger(module_name=os.path.splitext(os.path.basename(__file__))[0])


class JiraApiError(Exception):
    """Custom exception for errors related to Jira API requests."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class CacheError(Exception):
    """Custom exception for errors related to Cache operations."""

    def __init__(self, message):
        super().__init__(message)


def handle_request_exception(exception, context_message):
    """
    Handle exceptions that occur during requests to external APIs.

    :param exception: The exception raised by the request.
    :param context_message: Custom message providing context for the error.
    """
    if isinstance(exception, requests.exceptions.HTTPError):
        logger.error(
            (
                f"HTTP error occurred: {context_message} - Status Code: "
                f"{exception.response.status_code} - {exception}"
            )
        )
        raise JiraApiError(
            f"HTTP error occurred: {context_message}",
            status_code=exception.response.status_code,
        )
    elif isinstance(exception, requests.exceptions.ConnectionError):
        logger.error(f"Connection error occurred: {context_message} - {exception}")
        raise JiraApiError(f"Connection error occurred: {context_message}")
    elif isinstance(exception, requests.exceptions.Timeout):
        logger.error(f"Timeout error occurred: {context_message} - {exception}")
        raise JiraApiError(f"Timeout error occurred: {context_message}")
    elif isinstance(exception, requests.exceptions.RequestException):
        logger.error(f"Request error occurred: {context_message} - {exception}")
        raise JiraApiError(f"Request error occurred: {context_message}")
    else:
        logger.error(f"An unexpected error occurred: {context_message} - {exception}")
        raise JiraApiError(f"Unexpected error: {context_message}")


def handle_generic_exception(exception, context_message):
    """
    Handle generic exceptions that occur in the system.

    :param exception: The exception raised.
    :param context_message: Custom message providing context for the error.
    """
    logger.error(f"An error occurred: {context_message} - {exception}", exc_info=True)
    raise Exception(f"An error occurred: {context_message}")
