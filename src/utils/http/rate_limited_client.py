import random
import time
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.logging.logging_manager import LogManager


class RateLimitedHTTPClient:
    """HTTP client with rate limiting, retry logic, and session management
    Generic implementation for ethical web scraping with rate limiting
    """

    def __init__(self, rate_limit_seconds: float = 3.0, max_retries: int = 3):
        """Initialize HTTP client

        Args:
            rate_limit_seconds: Minimum seconds between requests
            max_retries: Maximum number of retries for failed requests
        """
        self.rate_limit = rate_limit_seconds
        self.max_retries = max_retries
        self.last_request_time = 0.0
        self.logger = LogManager.get_instance().get_logger("RateLimitedHTTPClient")

        # Initialize session with retry strategy
        self.session = self._create_session()

        # Statistics
        self.requests_made = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.rate_limit_delays = 0

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy and proper headers"""
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
            allowed_methods=["GET", "POST"],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Set default headers
        session.headers.update(self._get_browser_headers())

        return session

    def _get_browser_headers(self) -> dict[str, str]:
        """Get browser-like headers to avoid detection"""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def _apply_rate_limit(self) -> None:
        """Apply rate limiting with jitter"""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.rate_limit:
            # Add small random jitter to avoid synchronized requests
            sleep_time = self.rate_limit - time_since_last_request
            jitter = random.uniform(0.1, 0.5)
            total_sleep = sleep_time + jitter

            self.logger.debug(f"Rate limiting: sleeping for {total_sleep:.2f} seconds")
            time.sleep(total_sleep)
            self.rate_limit_delays += 1

        self.last_request_time = time.time()

    def get(self, url: str, timeout: int = 30, **kwargs) -> requests.Response:
        """Make GET request with rate limiting

        Args:
            url: URL to request
            timeout: Request timeout in seconds
            **kwargs: Additional arguments for requests.get

        Returns:
            Response object

        Raises:
            requests.RequestException: If request fails after all retries
        """
        self._apply_rate_limit()

        try:
            self.logger.debug(f"Making GET request to: {url}")
            self.requests_made += 1

            response = self.session.get(url, timeout=timeout, **kwargs)

            # Handle response status
            self._handle_response_status(response)

            self.successful_requests += 1
            self.logger.debug(f"Successful GET request: {url} (status: {response.status_code})")

            return response

        except requests.exceptions.RequestException as e:
            self.failed_requests += 1
            self.logger.error(f"Failed GET request to {url}: {e}")
            raise

    def post(
        self,
        url: str,
        data: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        timeout: int = 30,
        **kwargs,
    ) -> requests.Response:
        """Make POST request with rate limiting

        Args:
            url: URL to request
            data: Form data to send
            json: JSON data to send
            timeout: Request timeout in seconds
            **kwargs: Additional arguments for requests.post

        Returns:
            Response object

        Raises:
            requests.RequestException: If request fails after all retries
        """
        self._apply_rate_limit()

        try:
            self.logger.debug(f"Making POST request to: {url}")
            self.requests_made += 1

            response = self.session.post(url, data=data, json=json, timeout=timeout, **kwargs)

            # Handle response status
            self._handle_response_status(response)

            self.successful_requests += 1
            self.logger.debug(f"Successful POST request: {url} (status: {response.status_code})")

            return response

        except requests.exceptions.RequestException as e:
            self.failed_requests += 1
            self.logger.error(f"Failed POST request to {url}: {e}")
            raise

    def _handle_response_status(self, response: requests.Response) -> None:
        """Handle HTTP response status codes"""
        if response.status_code == 429:
            # Rate limited - increase delay for next request
            self.rate_limit = min(self.rate_limit * 1.5, 10.0)
            self.logger.warning(f"Rate limited (429). Increasing delay to {self.rate_limit:.2f}s")
            raise requests.exceptions.HTTPError(f"Rate limited: {response.status_code}")

        elif response.status_code == 403:
            self.logger.warning(f"Access forbidden (403) for URL: {response.url}")
            raise requests.exceptions.HTTPError(f"Access forbidden: {response.status_code}")

        elif response.status_code == 404:
            self.logger.warning(f"Page not found (404) for URL: {response.url}")
            raise requests.exceptions.HTTPError(f"Page not found: {response.status_code}")

        elif response.status_code >= 500:
            self.logger.warning(f"Server error ({response.status_code}) for URL: {response.url}")
            raise requests.exceptions.HTTPError(f"Server error: {response.status_code}")

        elif response.status_code >= 400:
            self.logger.warning(f"Client error ({response.status_code}) for URL: {response.url}")
            raise requests.exceptions.HTTPError(f"Client error: {response.status_code}")

        # Check for successful status codes
        response.raise_for_status()

    def get_statistics(self) -> dict[str, Any]:
        """Get client statistics"""
        return {
            "requests_made": self.requests_made,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": (self.successful_requests / max(self.requests_made, 1)) * 100,
            "rate_limit_delays": self.rate_limit_delays,
            "current_rate_limit": self.rate_limit,
        }

    def reset_statistics(self) -> None:
        """Reset statistics counters"""
        self.requests_made = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.rate_limit_delays = 0

    def close(self) -> None:
        """Close the session"""
        if self.session:
            self.session.close()
            self.logger.info("HTTP session closed")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
