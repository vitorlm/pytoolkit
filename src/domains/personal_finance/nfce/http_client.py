import requests

from utils.http.rate_limited_client import RateLimitedHTTPClient
from utils.logging.logging_manager import LogManager


class PortalSpedClient(RateLimitedHTTPClient):
    """Specialized HTTP client for Portal SPED MG"""

    def __init__(self, rate_limit_seconds: float = 3.0, max_retries: int = 3):
        super().__init__(rate_limit_seconds, max_retries)
        self.logger = LogManager.get_instance().get_logger("PortalSpedClient")

        # Portal SPED specific configuration
        self.base_url = "https://portalsped.fazenda.mg.gov.br"
        self.portal_session_started = False

    def get_invoice_page(self, url: str) -> requests.Response:
        """Get Portal SPED invoice page

        Args:
            url: Full Portal SPED URL

        Returns:
            Response object with invoice page content
        """
        if not url.startswith(self.base_url):
            raise ValueError(f"URL must be from Portal SPED MG: {url}")

        try:
            self.logger.info(f"Fetching invoice page: {url}")

            # Make request with Portal SPED specific headers
            response = self.get(url, headers=self._get_portal_sped_headers())

            # Check if we got a valid NFCe page
            if not self._is_valid_nfce_page(response):
                self.logger.warning(f"Response does not appear to be a valid NFCe page: {url}")

            return response

        except Exception as e:
            self.logger.error(f"Error fetching Portal SPED page {url}: {e}")
            raise

    def _get_portal_sped_headers(self) -> dict[str, str]:
        """Get Portal SPED specific headers"""
        headers = self._get_browser_headers()
        headers.update(
            {
                "Referer": self.base_url,
                "Origin": self.base_url,
                "Host": "portalsped.fazenda.mg.gov.br",
            }
        )
        return headers

    def _is_valid_nfce_page(self, response: requests.Response) -> bool:
        """Check if response contains valid NFCe content"""
        content = response.text.lower()

        # Look for NFCe indicators
        nfce_indicators = [
            "nota fiscal",
            "consumidor",
            "nfc-e",
            "nfce",
            "sefaz",
            "fazenda",
        ]

        return any(indicator in content for indicator in nfce_indicators)

    def test_connection(self) -> bool:
        """Test connection to Portal SPED"""
        try:
            self.logger.info("Testing connection to Portal SPED...")
            response = self.get(self.base_url, timeout=10)

            if response.status_code == 200:
                self.logger.info("Successfully connected to Portal SPED")
                return True
            else:
                self.logger.warning(f"Portal SPED returned status {response.status_code}")
                return False

        except Exception as e:
            self.logger.error(f"Failed to connect to Portal SPED: {e}")
            return False

    def fetch_nfce_page(self, url: str, timeout: int = 30) -> str:
        """Fetch NFCe page content

        Args:
            url: NFCe URL
            timeout: Request timeout

        Returns:
            HTML content as string
        """
        response = self.get_invoice_page(url)
        return response.text


# Alias for backward compatibility
NFCeHttpClient = PortalSpedClient
