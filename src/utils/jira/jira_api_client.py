import requests
from requests.auth import HTTPBasicAuth
from utils.jira.error import JiraApiRequestError
from utils.logging.logging_manager import LogManager


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
        self.logger = LogManager.get_instance().get_logger("JiraApiClient")
        self.base_url = base_url.rstrip("/") + "/rest/api/3/"
        self.auth = HTTPBasicAuth(email, api_token)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _handle_response(self, response):
        """
        Handle the HTTP response from the Jira API.

        Args:
            response (requests.Response): The HTTP response object.

        Returns:
            dict or None: Parsed JSON response, or None if no content.

        Raises:
            JiraApiRequestError: For unexpected status codes or invalid responses.
        """
        self.logger.debug(f"HTTP Status: {response.status_code}")
        self.logger.debug(f"Response Headers: {response.headers}")

        if response.status_code == 204:
            self.logger.info("Received 204 No Content.")
            return None

        if response.headers.get("Content-Type", "").startswith("application/json"):
            try:
                return response.json()
            except ValueError as e:
                self.logger.error(f"Failed to parse JSON response: {e}")
                self.logger.debug(f"Response Content: {response.content}")
                raise JiraApiRequestError(
                    message="Invalid JSON in response",
                    endpoint=response.url,
                    status_code=response.status_code,
                )

        # Handle unexpected content types
        self.logger.warning(f"Unexpected content type: {response.headers.get('Content-Type')}")
        return {"raw_response": response.content.decode("utf-8", errors="replace")}

    def _request(self, method: str, endpoint: str, **kwargs):
        """
        Make an HTTP request to the Jira API.

        Args:
            method (str): HTTP method ('GET', 'POST', 'PUT', etc.).
            endpoint (str): The API endpoint to call.

        Returns:
            dict or None: Parsed JSON response or None if no content.

        Raises:
            JiraApiRequestError: If the request fails or the response is invalid.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            self.logger.info(f"Sending {method.upper()} request to {url} with kwargs {kwargs}")
            response = requests.request(method, url, headers=self.headers, auth=self.auth, **kwargs)
            response.raise_for_status()
            return self._handle_response(response)
        except requests.RequestException as e:
            self.logger.error(f"{method.upper()} request failed: {e}")
            raise JiraApiRequestError(
                message=f"Failed to execute {method.upper()} request",
                endpoint=endpoint,
                params=kwargs.get("params"),
                payload=kwargs.get("json"),
                status_code=getattr(e.response, "status_code", None),
            )

    def get(self, endpoint: str, params: dict = None):
        """
        Make a GET request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.
            params (dict): Query parameters to include in the request (optional).

        Returns:
            dict or None: The JSON response from the API.
        """
        return self._request("GET", endpoint, params=params)

    def post(self, endpoint: str, payload: dict):
        """
        Make a POST request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.
            payload (dict): The JSON payload to send in the request body.

        Returns:
            dict or None: The JSON response from the API.
        """
        return self._request("POST", endpoint, json=payload)

    def put(self, endpoint: str, payload: dict):
        """
        Make a PUT request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.
            payload (dict): The JSON payload to send in the request body.

        Returns:
            dict or None: The JSON response from the API.
        """
        return self._request("PUT", endpoint, json=payload)

    def delete(self, endpoint: str):
        """
        Make a DELETE request to the Jira API.

        Args:
            endpoint (str): The API endpoint to call.

        Returns:
            dict or None: The JSON response from the API.
        """
        return self._request("DELETE", endpoint)
