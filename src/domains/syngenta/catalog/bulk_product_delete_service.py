import csv
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class BulkProductDeleteService:
    """Service for bulk deleting products from Cropwise Catalog API.

    This service provides comprehensive bulk deletion functionality with:
    - CSV data loading and validation
    - Batch processing with configurable batch sizes
    - Concurrent HTTP requests with retry logic
    - Comprehensive error handling and reporting
    - Dry-run mode for safe testing
    - Progress tracking and detailed logging
    """

    def __init__(self) -> None:
        """Initialize the bulk product delete service."""
        self.logger = LogManager.get_instance().get_logger("BulkProductDeleteService")

        # These will be set per execution
        self.session: Session | None = None

    def _setup_session(self) -> None:
        """Setup HTTP session with retry strategy."""
        self.session = requests.Session()
        retry_strategy = Retry(
            total=self.retry_attempts,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "DELETE"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        # Set default headers
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "User-Agent": "PyToolkit-BulkDelete/1.0",
            }
        )

    def load_products_from_csv(self, csv_path: str, country_filter: str | None = None) -> list[dict[str, Any]]:
        """Load product data from CSV file with validation and optional country filtering.

        Args:
            csv_path: Path to the CSV file containing product data
            country_filter: Optional country code to filter products (e.g., 'BR', 'US')

        Returns:
            list of product dictionaries with validated data

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV format is invalid, required columns are missing, or country not found
        """
        self.logger.info(f"Loading products from CSV: {csv_path}")
        if country_filter:
            self.logger.info(f"Filtering by country: {country_filter}")

        # Validate file exists and has correct extension
        FileManager.validate_file(csv_path, [".csv"])

        products = []
        required_columns = ["country", "product_id"]
        countries_found = set()
        country_filter_upper = country_filter.upper() if country_filter else None

        try:
            with open(csv_path, encoding="utf-8") as file:
                reader = csv.DictReader(file)

                # Validate required columns exist
                if reader.fieldnames is None:
                    raise ValueError("CSV file appears to be empty or malformed")

                fieldnames = reader.fieldnames  # Store to avoid repeated attribute access
                if not all(col in fieldnames for col in required_columns):
                    missing_cols = [col for col in required_columns if col not in fieldnames]
                    raise ValueError(f"Missing required columns in CSV: {missing_cols}")

                for row_num, row in enumerate(reader, start=2):
                    try:
                        # Validate product_id is a valid UUID
                        product_id = row["product_id"].strip()
                        uuid.UUID(product_id)

                        # Validate country code
                        country = row["country"].strip().upper()
                        if not country or len(country) != 2:
                            self.logger.warning(f"Row {row_num}: Invalid country code '{country}', using as-is")

                        countries_found.add(country)

                        # Apply country filter if specified
                        if country_filter_upper and country != country_filter_upper:
                            continue

                        products.append(
                            {
                                "country": country,
                                "product_id": product_id,
                                "product_name": row.get("product_name", "").strip(),
                                "trade_name": row.get("trade_name", "").strip(),
                                "api_name": row.get("api_name", "").strip(),
                                "row_number": row_num,
                            }
                        )

                    except ValueError as e:
                        self.logger.error(f"Row {row_num}: Invalid product_id format - {e}")
                        continue
                    except Exception as e:
                        self.logger.error(f"Row {row_num}: Error processing row - {e}")
                        continue

        except Exception as e:
            raise ValueError(f"Error reading CSV file: {e}")

        # Validate country filter exists in CSV if specified
        if country_filter_upper:
            if country_filter_upper not in countries_found:
                available_countries = sorted(list(countries_found))
                raise ValueError(
                    f"Country '{country_filter_upper}' not found in CSV file. "
                    f"Available countries: {', '.join(available_countries)}"
                )

        if not products:
            if country_filter_upper:
                raise ValueError(f"No valid products found for country '{country_filter_upper}' in CSV file")
            else:
                raise ValueError("No valid products found in CSV file")

        self.logger.info(f"Loaded {len(products)} valid products from CSV")
        if country_filter_upper:
            self.logger.info(f"Filtered to {len(products)} products for country: {country_filter_upper}")

        return products

    def delete_single_product(self, product: dict[str, Any], dry_run: bool = False) -> dict[str, Any]:
        """Delete a single product via API call.

        Args:
            product: Product data dictionary
            dry_run: If True, simulate deletion without actual API call

        Returns:
            Dictionary with deletion result and metadata
        """
        product_id = product["product_id"]
        country = product["country"]

        result = {
            "product_id": product_id,
            "country": country,
            "product_name": product.get("product_name", ""),
            "success": False,
            "error": None,
            "status_code": None,
            "response_time_ms": None,
            "timestamp": datetime.now().isoformat(),
        }

        if dry_run:
            result["success"] = True
            result["status_code"] = 200
            result["response_time_ms"] = 0
            self.logger.debug(f"DRY RUN: Would delete product {product_id} ({country})")
            return result

        # Ensure session is setup
        if self.session is None:
            self._setup_session()

        try:
            start_time = time.time()

            # Construct API endpoint URL
            delete_url = f"{self.api_base_url}/v2/catalog/products/{product_id}"

            self.logger.debug(f"Deleting product {product_id} from {delete_url}")

            # Make DELETE request
            if self.session is None:
                raise RuntimeError("Session not initialized. Call _setup_session() first.")
            response = self.session.delete(delete_url, timeout=30)

            end_time = time.time()
            result["response_time_ms"] = int((end_time - start_time) * 1000)
            result["status_code"] = response.status_code

            if response.status_code in [200, 204]:
                # Successful deletion
                result["success"] = True
                self.logger.debug(f"Successfully deleted product {product_id}")
            elif response.status_code == 404:
                # Product not found - this should NOT happen for products that exist in API
                result["success"] = False
                result["error"] = "Product not found in API (404)"
                self.logger.warning(
                    f"Product {product_id} not found - this is unexpected for products confirmed to exist"
                )
            else:
                result["error"] = f"HTTP {response.status_code}: {response.text[:200]}"
                self.logger.error(f"Failed to delete product {product_id}: {result['error']}")

        except requests.exceptions.Timeout:
            result["error"] = "Request timeout"
            self.logger.error(f"Timeout deleting product {product_id}")
        except requests.exceptions.RequestException as e:
            result["error"] = f"Request error: {str(e)[:200]}"
            self.logger.error(f"Request error deleting product {product_id}: {e}")
        except Exception as e:
            result["error"] = f"Unexpected error: {str(e)[:200]}"
            self.logger.error(f"Unexpected error deleting product {product_id}: {e}")

        return result

    def delete_batch(
        self, products: list[dict[str, Any]], batch_num: int, dry_run: bool = False
    ) -> list[dict[str, Any]]:
        """Delete a batch of products using concurrent requests.

        Args:
            products: list of product dictionaries to delete
            batch_num: Batch number for logging
            dry_run: If True, simulate deletions without actual API calls

        Returns:
            list of deletion results
        """
        self.logger.info(f"Processing batch {batch_num} with {len(products)} products")

        results = []

        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(products))) as executor:
            # Submit all deletion tasks
            future_to_product = {
                executor.submit(self.delete_single_product, product, dry_run): product for product in products
            }

            # Collect results as they complete
            for future in as_completed(future_to_product):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    product = future_to_product[future]
                    self.logger.error(f"Failed to process product {product['product_id']}: {e}")
                    results.append(
                        {
                            "product_id": product["product_id"],
                            "country": product["country"],
                            "product_name": product.get("product_name", ""),
                            "success": False,
                            "error": f"Processing error: {str(e)[:200]}",
                            "status_code": None,
                            "response_time_ms": None,
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

        successful = len([r for r in results if r["success"]])
        failed = len(results) - successful

        self.logger.info(f"Batch {batch_num} completed: {successful} successful, {failed} failed")

        return results

    def generate_reports(
        self, all_results: list[dict[str, Any]], execution_summary: dict[str, Any]
    ) -> dict[str, str | None]:
        """Generate comprehensive reports in JSON and CSV formats.

        Args:
            all_results: list of all deletion results
            execution_summary: Summary of the execution

        Returns:
            Dictionary with paths to generated report files
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate detailed JSON report
        json_report_path = os.path.join(self.output_dir, f"bulk_delete_report_{timestamp}.json")
        json_report = {
            "execution_summary": execution_summary,
            "detailed_results": all_results,
            "statistics": {
                "total_processed": len(all_results),
                "successful_deletions": len(
                    [r for r in all_results if r["success"] and r["status_code"] in [200, 204]]
                ),
                "products_not_found_404": len([r for r in all_results if r["status_code"] == 404]),
                "failed_deletions": len([r for r in all_results if not r["success"]]),
                "success_rate": round(
                    len([r for r in all_results if r["success"] and r["status_code"] in [200, 204]])
                    / len(all_results)
                    * 100,
                    2,
                )
                if all_results
                else 0,
                "average_response_time_ms": round(
                    sum(r.get("response_time_ms", 0) for r in all_results if r.get("response_time_ms"))
                    / len([r for r in all_results if r.get("response_time_ms")]),
                    2,
                )
                if any(r.get("response_time_ms") for r in all_results)
                else 0,
            },
        }

        with open(json_report_path, "w", encoding="utf-8") as f:
            json.dump(json_report, f, indent=2, ensure_ascii=False)

        # Generate CSV report
        csv_report_path = os.path.join(self.output_dir, f"bulk_delete_results_{timestamp}.csv")
        if all_results:
            with open(csv_report_path, "w", newline="", encoding="utf-8") as f:
                fieldnames = all_results[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_results)

        # Generate failures-only CSV for retry purposes
        failures = [r for r in all_results if not r["success"]]
        failures_csv_path = None
        if failures:
            failures_csv_path = os.path.join(self.output_dir, f"failed_deletions_{timestamp}.csv")
            with open(failures_csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["country", "product_id", "product_name", "error"])
                writer.writeheader()
                for failure in failures:
                    writer.writerow(
                        {
                            "country": failure["country"],
                            "product_id": failure["product_id"],
                            "product_name": failure["product_name"],
                            "error": failure["error"],
                        }
                    )

        self.logger.info("Reports generated:")
        self.logger.info(f"  - JSON report: {json_report_path}")
        self.logger.info(f"  - CSV report: {csv_report_path}")
        if failures:
            self.logger.info(f"  - Failures CSV: {failures_csv_path}")

        return {
            "json_report": json_report_path,
            "csv_report": csv_report_path,
            "failures_csv": failures_csv_path if failures else None,
        }

    def execute_bulk_delete(
        self,
        csv_path: str,
        country_filter: str | None,
        api_base_url: str,
        api_key: str,
        batch_size: int = 100,
        output_dir: str = "output/bulk-delete-results",
        retry_attempts: int = 3,
        delay_between_batches: float = 1.0,
        max_workers: int = 10,
        dry_run: bool = False,
        skip_confirmation: bool = False,
    ) -> dict[str, Any]:
        """Execute the complete bulk deletion process.

        Args:
            csv_path: Path to CSV file containing products to delete
            country_filter: Optional country code to filter products (e.g., 'BR', 'US')
            api_base_url: Base URL for the Cropwise API
            api_key: API key for authentication
            batch_size: Number of products to process per batch
            output_dir: Directory for output reports and logs
            retry_attempts: Number of retry attempts for failed requests
            delay_between_batches: Delay in seconds between batch processing
            max_workers: Maximum number of concurrent workers
            dry_run: If True, simulate deletions without actual API calls
            skip_confirmation: If True, skip interactive confirmation

        Returns:
            Dictionary with execution results and statistics
        """
        start_time = datetime.now()

        # Set configuration for this execution
        self.api_base_url = api_base_url.rstrip("/")
        self.api_key = api_key
        self.batch_size = batch_size
        self.output_dir = output_dir
        self.retry_attempts = retry_attempts
        self.delay_between_batches = delay_between_batches
        self.max_workers = max_workers

        # Create output directory
        FileManager.create_folder(self.output_dir)

        # Setup HTTP session with retry strategy
        self._setup_session()

        try:
            # Load products from CSV
            products = self.load_products_from_csv(csv_path, country_filter)

            self.logger.info(f"Loaded {len(products)} products for {'simulation' if dry_run else 'deletion'}")
            if country_filter:
                self.logger.info(f"Country filter applied: {country_filter.upper()}")

            # Show confirmation unless skipped
            if not skip_confirmation and not dry_run:
                filter_text = f" from country {country_filter.upper()}" if country_filter else ""
                print(f"\n🚨 WARNING: About to delete {len(products)} products{filter_text}!")
                print(f"API Base URL: {self.api_base_url}")
                print(f"Batch size: {self.batch_size}")
                print(f"Output directory: {self.output_dir}")

                response = input("\nDo you want to proceed? (yes/no): ").lower().strip()
                if response not in ["yes", "y"]:
                    self.logger.info("Bulk deletion cancelled by user")
                    return {"cancelled": True}

            # Process products in batches
            all_results = []
            total_batches = (len(products) + self.batch_size - 1) // self.batch_size

            self.logger.info(f"Processing {len(products)} products in {total_batches} batches")

            for i in range(0, len(products), self.batch_size):
                batch_num = (i // self.batch_size) + 1
                batch = products[i : i + self.batch_size]

                # Process batch
                batch_results = self.delete_batch(batch, batch_num, dry_run)
                all_results.extend(batch_results)

                # Add delay between batches (except for last batch)
                if i + self.batch_size < len(products):
                    time.sleep(self.delay_between_batches)

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            # Calculate statistics
            successful_deletions = len([r for r in all_results if r["success"] and r["status_code"] in [200, 204]])
            not_found_404 = len([r for r in all_results if r["status_code"] == 404])
            failed = len([r for r in all_results if not r["success"]])

            execution_summary = {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "execution_time_seconds": round(execution_time, 2),
                "total_products": len(products),
                "total_batches": total_batches,
                "batch_size": self.batch_size,
                "successful_deletions": successful_deletions,
                "products_not_found_404": not_found_404,
                "failed_deletions": failed,
                "success_rate_percent": round(successful_deletions / len(all_results) * 100, 2) if all_results else 0,
                "dry_run": dry_run,
                "api_base_url": self.api_base_url,
            }

            # Generate reports
            report_paths = self.generate_reports(all_results, execution_summary)

            # Log final summary
            self.logger.info(f"Bulk deletion {'simulation' if dry_run else 'process'} completed")
            self.logger.info(f"Execution time: {execution_time:.2f} seconds")
            self.logger.info(f"Success rate: {execution_summary['success_rate_percent']:.2f}%")

            return {
                "success": True,
                "execution_summary": execution_summary,
                "total_processed": len(all_results),
                "successful_deletions": successful_deletions,
                "products_not_found_404": not_found_404,
                "failed_deletions": failed,
                "report_paths": report_paths,
            }

        except Exception as e:
            self.logger.error(f"Bulk deletion failed: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "total_processed": 0,
                "successful_deletions": 0,
                "failed_deletions": 0,
            }
