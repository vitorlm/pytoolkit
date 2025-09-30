"""
CW Catalog API Client

API client for Cropwise Catalog API to fetch products by country and organization.
Based on the API specification at workspaces/api/src/swagger-spec.yaml.
"""

import json
import time
from typing import Dict, List, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.logging.logging_manager import LogManager


class CWCatalogApiClient:
    """Client for Cropwise Catalog API."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        """
        Initialize the CW Catalog API client.

        Args:
            base_url: The base URL of the catalog API (e.g., https://api.cropwise.com)
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.logger = LogManager.get_instance().get_logger("CWCatalogApiClient")

        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set default headers
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def get_products_by_country(
        self,
        country: str,
        org_id: Optional[str] = None,
        source: str = "TUBE",
        include_deleted: bool = False,
        full: bool = True,
        indication: Optional[str] = None,
        name: Optional[str] = None,
        size: int = 1000,
        include_attributes: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Get products by country and optionally by organization.

        Args:
            country: ISO2 country code (e.g., 'BR', 'AR', 'US')
            org_id: Organization UUID (optional - if provided, gets org-specific products)
            source: Product source (default: 'TUBE')
            include_deleted: Include deleted products
            full: Return full product attributes
            indication: Filter by product indication
            name: Filter by product name (begins_with search)
            size: Page size (max 1000)
            include_attributes: Specific attributes to include

        Returns:
            API response with products list and pagination info
        """
        # Use different endpoint based on whether org_id is provided
        if org_id:
            # Organization-specific products endpoint
            endpoint = f"/v2/catalog/orgs/{org_id}/products"
        else:
            # Canonical products endpoint
            endpoint = "/v2/catalog/products"

        params = {
            "country": country,
            "source": source,
            "include_deleted": include_deleted,
            "full": full,
            "size": size,
        }

        if indication:
            params["indication"] = indication
        if name:
            params["name"] = name
        if include_attributes:
            params["include_attributes"] = include_attributes

        return self._make_request("GET", endpoint, params=params)

    def get_all_products_by_country(
        self,
        country: str,
        org_id: Optional[str] = None,
        source: str = "TUBE",
        include_deleted: bool = False,
        batch_size: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Get all products for a country using pagination.

        Args:
            country: ISO2 country code
            org_id: Organization UUID (optional - for org-specific products)
            source: Product source
            include_deleted: Include deleted products
            batch_size: Number of products per API call

        Returns:
            Complete list of all products for the country
        """
        all_products = []
        current_key = None
        page = 1

        endpoint_type = "organization-specific" if org_id else "canonical"
        self.logger.info(
            f"Fetching all {endpoint_type} products for country {country} from source {source}"
        )

        while True:
            self.logger.info(f"Fetching page {page} (batch size: {batch_size})")

            params = {
                "country": country,
                "org_id": org_id,
                "source": source,
                "include_deleted": include_deleted,
                "full": True,
                "size": batch_size,
            }

            if current_key:
                params["current_key"] = current_key

            try:
                response = self.get_products_by_country(**params)

                # Extract products from response
                products = response.get("content", [])
                all_products.extend(products)

                self.logger.info(
                    f"Page {page}: Retrieved {len(products)} products (total: {len(all_products)})"
                )

                # Check if there are more pages
                pagination = response.get("pagination", {})
                current_key = pagination.get("next_key")

                if not current_key or len(products) == 0:
                    break

                page += 1

                # Add small delay to be respectful to the API
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(
                    f"Error fetching page {page} for country {country}: {e}"
                )
                break

        self.logger.info(
            f"Completed fetching products for {country}: {len(all_products)} total products"
        )
        return all_products

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to the API.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            data: Request body data

        Returns:
            JSON response

        Raises:
            requests.RequestException: If request fails
        """
        url = f"{self.base_url}{endpoint}"

        self.logger.debug(f"Making {method} request to {url}")

        try:
            response = self.session.request(
                method=method, url=url, params=params, json=data, timeout=self.timeout
            )

            response.raise_for_status()

            # Parse JSON response
            result = response.json()

            self.logger.debug(f"Request successful: {response.status_code}")
            return result

        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error {e.response.status_code}: {e.response.text}"
            self.logger.error(error_msg)
            raise requests.RequestException(error_msg) from e

        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {e}"
            self.logger.error(error_msg)
            raise

        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse JSON response: {e}"
            self.logger.error(error_msg)
            raise requests.RequestException(error_msg) from e

    def get_products_by_ids(
        self, product_ids: List[str], include_deleted: bool = False, full: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Get products by their IDs using the /products/ids endpoint.

        Args:
            product_ids: List of product IDs to fetch
            include_deleted: Whether to include deleted products
            full: Whether to return full product details

        Returns:
            List of products matching the provided IDs
        """
        if not product_ids:
            return []

        self.logger.info(f"Fetching {len(product_ids)} products by IDs")

        endpoint = "/v2/catalog/products/ids"
        params = {"include_deleted": include_deleted, "full": full}

        data = {"ids": product_ids}

        try:
            response = self._make_request("POST", endpoint, params=params, data=data)
            # Log the equivalent cURL command for debugging
            curl_command = f"curl -X POST '{self.base_url}{endpoint}"
            if params:
                param_strings = [f"{k}={v}" for k, v in params.items()]
                curl_command += f"?{'&'.join(param_strings)}"
            curl_command += f"' \\\n  -H 'Authorization: Bearer {self.api_key}' \\\n  -H 'Content-Type: application/json' \\\n  -H 'Accept: application/json' \\\n  -d '{json.dumps(data)}'"

            self.logger.debug(f"Equivalent cURL command:\n{curl_command}")
            products = response.get("products", [])

            self.logger.info(f"Successfully fetched {len(products)} products")
            return products

        except Exception as e:
            self.logger.error(f"Failed to fetch products by IDs: {e}")
            raise

    def get_products_by_ids_in_batches(
        self,
        product_ids: List[str],
        batch_size: int = 100,
        include_deleted: bool = False,
        full: bool = True,
        delay_between_batches: float = 0.5,
    ) -> List[Dict[str, Any]]:
        """
        Get products by IDs in batches to handle large lists efficiently.

        Args:
            product_ids: List of product IDs to fetch
            batch_size: Number of IDs per batch
            include_deleted: Whether to include deleted products
            full: Whether to return full product details
            delay_between_batches: Delay in seconds between batch requests

        Returns:
            List of all products found across all batches
        """
        if not product_ids:
            return []

        self.logger.info(
            f"Fetching {len(product_ids)} products in batches of {batch_size}"
        )

        all_products = []
        total_batches = (len(product_ids) + batch_size - 1) // batch_size

        for i in range(0, len(product_ids), batch_size):
            batch_ids = product_ids[i : i + batch_size]
            batch_num = (i // batch_size) + 1

            self.logger.debug(
                f"Processing batch {batch_num}/{total_batches} ({len(batch_ids)} IDs)"
            )

            try:
                batch_products = self.get_products_by_ids(
                    product_ids=batch_ids, include_deleted=include_deleted, full=full
                )
                all_products.extend(batch_products)

                # Add delay between batches to avoid rate limiting
                if batch_num < total_batches and delay_between_batches > 0:
                    time.sleep(delay_between_batches)

            except Exception as e:
                self.logger.error(f"Failed to process batch {batch_num}: {e}")
                # Continue with other batches instead of failing completely
                continue

        self.logger.info(
            f"Successfully fetched {len(all_products)} products from {total_batches} batches"
        )
        return all_products

    def health_check(self) -> bool:
        """
        Perform a health check against the API.

        Returns:
            True if API is healthy, False otherwise
        """
        try:
            endpoint = "/health"
            self._make_request("GET", endpoint)
            self.logger.info("API health check passed")
            return True
        except Exception as e:
            self.logger.error(f"API health check failed: {e}")
            return False
