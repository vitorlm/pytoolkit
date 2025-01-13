import requests
from requests.auth import HTTPBasicAuth
from utils.jira.error import JiraApiRequestError
from utils.logging.logging_manager import LogManager

# Configure logging
logger = LogManager.get_instance().get_logger("JiraApiClient")


class JiraApiClient:
    """
    A robust Jira API Client to handle basic API operations with enhanced error handling and logging
    """

    def __init__(self, base_url: str, email: str, api_token: str):
        """
        Initialize the Jira API client.

        Args:
            base_url (str): The base URL of the Jira API.
            email (str): The email address used for authentication.
            api_token (str): The API token used for authentication.
        """
        self.base_url = base_url.rstrip("/") + "/rest/api/3/"
        self.auth = HTTPBasicAuth(email, api_token)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get(self, endpoint: str, params: dict = None):
        """
        Make a GET request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.
            params (dict): Query parameters to include in the request (optional).

        Returns:
            dict: The JSON response from the API.

        Raises:
            JiraApiRequestError: If the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            logger.info(f"Sending GET request to {url} with params {params}")
            response = requests.get(url, headers=self.headers, params=params, auth=self.auth)
            response.raise_for_status()
            logger.debug(f"GET response: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"GET request failed: {e}")
            raise JiraApiRequestError(
                message="Failed to execute GET request",
                endpoint=endpoint,
                params=params,
                status_code=response.status_code if "response" in locals() else None,
            )

    def post(self, endpoint: str, payload: dict):
        """
        Make a POST request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.
            payload (dict): The JSON payload to send in the request body.

        Returns:
            dict: The JSON response from the API.

        Raises:
            JiraApiRequestError: If the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            logger.info(f"Sending POST request to {url} with payload {payload}")
            response = requests.post(url, headers=self.headers, json=payload, auth=self.auth)
            response.raise_for_status()
            logger.debug(f"POST response: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"POST request failed: {e}")
            raise JiraApiRequestError(
                message="Failed to execute POST request",
                endpoint=endpoint,
                payload=payload,
                status_code=response.status_code if "response" in locals() else None,
            )

    def put(self, endpoint: str, payload: dict):
        """
        Make a PUT request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.
            payload (dict): The JSON payload to send in the request body.

        Returns:
            dict: The JSON response from the API.

        Raises:
            JiraApiRequestError: If the request fails.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            logger.info(f"Sending PUT request to {url} with payload {payload}")
            response = requests.put(url, headers=self.headers, json=payload, auth=self.auth)
            response.raise_for_status()
            logger.debug(f"PUT response: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"PUT request failed: {e}")
            raise JiraApiRequestError(
                message="Failed to execute PUT request",
                endpoint=endpoint,
                payload=payload,
                status_code=response.status_code if "response" in locals() else None,
            )
        except Exception as e:
            logger.error(f"PUT request failed: {e}")
            raise JiraApiRequestError(
                message="An unexpected error occurred during PUT request",
                endpoint=endpoint,
                payload=payload,
                status_code=None,
            )
