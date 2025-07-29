"""
CW Catalog API Comparison Service

Service to download products from CW Catalog API by country and compare against CSV files.
This addresses the data reconciliation requirement to understand what's in the API vs CSV.
"""

import csv
import os
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict

from utils.logging.logging_manager import LogManager
from utils.file_manager import FileManager
from utils.data.json_manager import JSONManager
from utils.cache_manager.cache_manager import CacheManager
from utils.api.cw_catalog_api_client import CWCatalogApiClient
from .config import CatalogConfig
import uuid


class CWCatalogComparisonService:
    """Service for comparing CSV data against CW Catalog API products."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("CWCatalogComparisonService")
        self.cache = CacheManager.get_instance()
        self.config = CatalogConfig

    def _apply_config_defaults(self, **kwargs) -> Dict[str, Any]:
        """Apply configuration defaults to parameters."""
        return {
            "api_base_url": kwargs.get("api_base_url") or self.config.CROPWISE_API_BASE_URL,
            "api_key": kwargs.get("api_key") or self.config.CROPWISE_API_KEY,
            "org_id": kwargs.get("org_id") or self.config.DEFAULT_ORG_ID,
            "source": kwargs.get("source") or self.config.DEFAULT_SOURCE,
            "batch_size": kwargs.get("batch_size") or self.config.DEFAULT_BATCH_SIZE,
            "cache_duration_minutes": kwargs.get("cache_duration_minutes") or self.config.DEFAULT_CACHE_DURATION,
            **{
                k: v
                for k, v in kwargs.items()
                if k not in ["api_base_url", "api_key", "org_id", "source", "batch_size", "cache_duration_minutes"]
            },
        }

    def _validate_api_config(self, api_base_url: str, api_key: str):
        """Validate required API configuration."""
        if not api_base_url:
            raise ValueError("API base URL is required (set CROPWISE_API_BASE_URL)")
        if not api_key:
            raise ValueError("API key is required (set CROPWISE_API_KEY)")

    def compare_csv_against_api_by_ids(
        self,
        csv_path: str,
        country_filter: Optional[str] = None,
        output_path: Optional[str] = None,
        include_deleted: bool = True,
        **config_overrides,
    ) -> Dict[str, Any]:
        """
        Compare CSV products against CW Catalog API by searching for CSV product IDs directly.
        This is much more efficient than downloading all products from a country.

        Args:
            csv_path: Path to CSV file with products to check
            country_filter: ISO2 country code to filter CSV products (e.g., 'BR', 'AR', 'US')
            api_base_url: Base URL for CW Catalog API (uses config default if None)
            api_key: API authentication key (uses config default if None)
            output_path: Optional path to save detailed report
            include_deleted: Include deleted products in API search
            batch_size: Number of product IDs per API call
            cache_duration_minutes: Cache duration for API responses

        Returns:
            Dictionary with comparison results showing which CSV products exist in API
        """
        country_msg = f" for country: {country_filter}" if country_filter else " without country filter"
        self.logger.info(f"Starting optimized CSV vs API comparison{country_msg}")

        # Apply configuration defaults
        config = self._apply_config_defaults(
            country_filter=country_filter, output_path=output_path, include_deleted=include_deleted, **config_overrides
        )

        # Validate required parameters
        self._validate_api_config(config["api_base_url"], config["api_key"])
        FileManager.validate_file(csv_path, allowed_extensions=[".csv"])

        # Initialize API client
        api_client = CWCatalogApiClient(config["api_base_url"], config["api_key"])

        csv_result = self._analyze_csv(csv_path, country_filter)
        csv_stats = csv_result["stats"]

        if csv_stats["total_products"] == 0:
            raise ValueError(f"No products found in CSV file: {csv_path}")

        if not country_filter:
            self.logger.info(f"Found {csv_stats['total_products']} products in CSV (all countries)")
        else:
            self.logger.info(f"Found {csv_stats['total_products']} products in CSV for country '{country_filter}'")

        # Build unique CSV IDs per country (always available in products_by_country)
        unique_csv_ids_by_country = {
            country: {prod["id"].lower() for prod in prod_list if prod.get("id")}
            for country, prod_list in csv_result["products_by_country"].items()
        }

        api_products_by_country = {}
        for country, ids in unique_csv_ids_by_country.items():
            self.logger.info(f"Processing country '{country}' with {len(ids)} unique product IDs from CSV")
            cache_key = f"api_products_by_ids_{country}_{len(ids)}_{include_deleted}"
            cached_products = self.cache.load(cache_key, expiration_minutes=config["cache_duration_minutes"])

            if cached_products is None:
                country_products = api_client.get_products_by_ids_in_batches(
                    product_ids=list(ids),
                    batch_size=config["batch_size"],
                    include_deleted=include_deleted,
                    full=True,
                    delay_between_batches=0.5,
                )
                # Cache only after successfully reading all products from the country
                self.cache.save(cache_key, country_products)
                api_products_by_country[country] = country_products
                self.logger.info(f"Downloaded and cached {len(country_products)} products for country '{country}'")
            else:
                api_products_by_country[country] = cached_products
                self.logger.info(f"Using cached data: {len(cached_products)} products for country '{country}'")

        # Perform comparison for each country
        country_comparisons = {}
        overall_stats = {
            "total_csv_products": csv_stats["total_products"],
            "total_api_products": 0,
            "products_found_in_api": 0,
            "products_not_found_in_api": 0,
            "products_ready_for_deletion": 0,
            "products_already_deleted": 0,
        }

        for country, csv_country_products in csv_result["products_by_country"].items():
            api_country_products = api_products_by_country.get(country, [])
            overall_stats["total_api_products"] += len(api_country_products)

            # Perform deletion status analysis for this country
            country_comparison = self._analyze_deletion_status(
                csv_products=csv_country_products, api_products=api_country_products, country_filter=country
            )
            country_comparisons[country] = country_comparison

            # Update overall stats
            overall_stats["products_found_in_api"] += country_comparison["summary"]["products_found_in_api"]
            overall_stats["products_not_found_in_api"] += country_comparison["summary"]["products_not_found_in_api"]
            overall_stats["products_ready_for_deletion"] += country_comparison["summary"]["products_ready_for_deletion"]
            overall_stats["products_already_deleted"] += country_comparison["summary"]["products_already_deleted"]

        # Build complete comparison result
        comparison_result = {
            "summary": overall_stats,
            "country_comparisons": country_comparisons,
            "api_products_by_country": api_products_by_country,
            "csv_stats": csv_stats,
        }

        # Add metadata
        comparison_result["metadata"] = {
            "timestamp": datetime.now().isoformat(),
            "country_filter": country_filter,
            "api_base_url": config["api_base_url"],
            "include_deleted": include_deleted,
            "batch_size": config["batch_size"],
            "comparison_method": "id_based_search",
            "csv_file": csv_path,
        }

        # Save results if output path provided
        if config["output_path"]:
            self._save_id_comparison_report(comparison_result, config["output_path"])

        return comparison_result

    def _analyze_deletion_status(
        self, csv_products: List[Dict[str, str]], api_products: List[Dict[str, Any]], country_filter: str
    ) -> Dict[str, Any]:
        """
        Analyze deletion status of CSV products against API results.

        Since we search by CSV IDs, we know all returned API products exist.
        The key analysis is identifying which products are already deleted vs ready for deletion.

        Args:
            csv_products: Products from CSV for the specified country
            api_products: Products found in API by ID search
            country_filter: Country code being processed

        Returns:
            Analysis results focused on deletion status
        """
        self.logger.info(f"Analyzing deletion status for {country_filter}")

        # Create lookup dictionary for API products by ID
        api_products_by_id = {product.get("id", "").lower(): product for product in api_products}

        # Track different categories based on deletion status
        products_ready_for_deletion = []  # CSV products found in API and NOT deleted
        products_already_deleted = []  # CSV products found in API and already deleted
        products_not_found_in_api = []  # CSV products not found in API

        for csv_product in csv_products:
            csv_id = csv_product["id"].lower()

            if csv_id in api_products_by_id:
                api_product = api_products_by_id[csv_id]
                api_deleted = api_product.get("deleted", False)
                api_status = api_product.get("status", "")

                product_info = {
                    "csv_product": csv_product,
                    "api_product": api_product,
                    "id": csv_id,
                    "trade_name": csv_product.get("trade_name", ""),
                    "api_name": api_product.get("name", ""),
                    "api_status": api_status,
                    "api_deleted": api_deleted,
                }

                if api_deleted:
                    products_already_deleted.append(product_info)
                else:
                    products_ready_for_deletion.append(product_info)
            else:
                products_not_found_in_api.append(csv_product)

        # Calculate statistics
        total_csv = len(csv_products)
        found_count = len(products_ready_for_deletion) + len(products_already_deleted)
        ready_for_deletion_count = len(products_ready_for_deletion)
        already_deleted_count = len(products_already_deleted)
        not_found_count = len(products_not_found_in_api)

        result = {
            "country_code": country_filter,
            "summary": {
                "total_csv_products": total_csv,
                "products_found_in_api": found_count,
                "products_not_found_in_api": not_found_count,
                "products_ready_for_deletion": ready_for_deletion_count,
                "products_already_deleted": already_deleted_count,
                "found_percentage": (found_count / total_csv * 100) if total_csv > 0 else 0,
                "already_deleted_percentage": (already_deleted_count / total_csv * 100) if total_csv > 0 else 0,
                "ready_for_deletion_percentage": (ready_for_deletion_count / total_csv * 100) if total_csv > 0 else 0,
            },
            "products_ready_for_deletion": products_ready_for_deletion,
            "products_already_deleted": products_already_deleted,
            "products_not_found_in_api": products_not_found_in_api,
        }

        self.logger.info(
            f"Deletion analysis completed for {country_filter}: "
            f"{ready_for_deletion_count} ready for deletion, "
            f"{already_deleted_count} already deleted, "
            f"{not_found_count} not found in API"
        )

        return result

    def _save_id_comparison_report(self, comparison_result: Dict[str, Any], output_path: str):
        """Save comprehensive ID-based comparison report."""
        output_dir = os.path.dirname(output_path)
        if output_dir:
            FileManager.create_folder(output_dir)

        base_name = os.path.splitext(output_path)[0]

        # Save main report and summary CSV
        JSONManager.write_json(comparison_result, output_path)
        self.logger.info(f"ID-based comparison report saved to: {output_path}")

        summary_csv_path = f"{base_name}_summary.csv"
        self._save_id_comparison_summary_csv(comparison_result, summary_csv_path)

    def _save_id_comparison_summary_csv(self, comparison_result: Dict[str, Any], csv_path: str):
        """Save summary CSV for deletion status analysis."""
        fieldnames = [
            "country_code",
            "total_csv_products",
            "products_found_in_api",
            "products_not_found_in_api",
            "products_ready_for_deletion",
            "products_already_deleted",
            "found_percentage",
            "ready_for_deletion_percentage",
            "already_deleted_percentage",
            "comparison_method",
        ]

        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for country, country_comparison in comparison_result["country_comparisons"].items():
                summary = country_comparison["summary"]
                writer.writerow(
                    {
                        "country_code": country,
                        "total_csv_products": summary["total_csv_products"],
                        "products_found_in_api": summary["products_found_in_api"],
                        "products_not_found_in_api": summary["products_not_found_in_api"],
                        "products_ready_for_deletion": summary["products_ready_for_deletion"],
                        "products_already_deleted": summary["products_already_deleted"],
                        "found_percentage": f"{summary['found_percentage']:.1f}%",
                        "ready_for_deletion_percentage": f"{summary['ready_for_deletion_percentage']:.1f}%",
                        "already_deleted_percentage": f"{summary['already_deleted_percentage']:.1f}%",
                        "comparison_method": "deletion_status_analysis",
                    }
                )

    def compare_csv_against_api(
        self,
        csv_path: str,
        api_base_url: str,
        api_key: str,
        country_filter: str,
        use_optimized_search: bool = True,
        **kwargs,
    ) -> Dict[str, Any]:
        """Compare CSV products against CW Catalog API products for a specific country."""
        if use_optimized_search:
            self.logger.info("Using optimized ID-based search method")
            return self.compare_csv_against_api_by_ids(
                csv_path=csv_path,
                country_filter=country_filter,
                api_base_url=api_base_url,
                api_key=api_key,
                batch_size=min(kwargs.get("batch_size", 1000), 100),
                **{k: v for k, v in kwargs.items() if k != "batch_size"},
            )

        self.logger.info("Using original method - downloading all products from country")
        return self._compare_csv_against_api_original(
            csv_path=csv_path, api_base_url=api_base_url, api_key=api_key, country_filter=country_filter, **kwargs
        )

    def _compare_csv_against_api_original(
        self,
        csv_path: str,
        api_base_url: str,
        api_key: str,
        country_filter: str,
        org_id: Optional[str] = None,
        source: str = "TUBE",
        output_path: Optional[str] = None,
        include_deleted: bool = True,
        batch_size: int = 1000,
        cache_duration_minutes: int = 60,
    ) -> Dict[str, Any]:
        """
        Original implementation: Compare CSV products against CW Catalog API by downloading all country products.

        This method downloads all products from a country and then compares against CSV.
        Less efficient for large datasets but provides complete country overview.
        """
        self.logger.info("Starting CSV vs CW Catalog API comparison")

        # Validate inputs
        FileManager.validate_file(csv_path, allowed_extensions=[".csv"])

        # Initialize API client
        api_client = CWCatalogApiClient(api_base_url, api_key)

        # Read and analyze CSV data, filtering by the specified country
        csv_result = self._analyze_csv(csv_path, country_filter)
        csv_products_by_country = csv_result["products_by_country"]
        csv_stats = csv_result["stats"]

        # Validate that the specified country has products in the CSV
        if country_filter not in csv_products_by_country:
            raise ValueError(
                f"No products found for country '{country_filter}' in CSV file. "
                f"Available countries: {list(csv_products_by_country.keys())}"
            )

        self.logger.info(f"CSV analysis for {country_filter}: {len(csv_products_by_country[country_filter])} products")

        # Download API products for the specified country only
        api_results = {}
        comparison_results = {
            "csv_stats": csv_stats,
            "api_stats": {},
            "country_comparisons": {},
            "overall_comparison": {
                "csv_total": csv_stats["total_products"],
                "api_total": 0,
                "matched_by_id": 0,
                "matched_by_name_country": 0,
                "csv_only": 0,
                "api_only": 0,
                "countries_processed": 0,
            },
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "source": source,
                "include_deleted": include_deleted,
                "org_id": org_id,
                "country_filter": country_filter,
                "api_base_url": api_base_url,
            },
        }

        # Process only the specified country
        self.logger.info(f"Processing country: {country_filter}")

        try:
            # Check cache first
            cache_key = f"api_products_{org_id}_{country_filter}_{source}_{include_deleted}"
            api_products = self.cache.load(cache_key, expiration_minutes=cache_duration_minutes)

            if api_products is None:
                # Download from API
                api_products = api_client.get_all_products_by_country(
                    country=country_filter,
                    org_id=org_id,
                    source=source,
                    include_deleted=include_deleted,
                    batch_size=batch_size,
                )
                # Cache the results
                self.cache.save(cache_key, api_products)
                self.logger.info(f"Downloaded and cached {len(api_products)} products for {country_filter}")
            else:
                self.logger.info(f"Using cached data: {len(api_products)} products for {country_filter}")

            api_results[country_filter] = api_products

            # Compare country data
            country_comparison = self._compare_country_data(
                csv_products_by_country[country_filter], api_products, country_filter
            )
            comparison_results["country_comparisons"][country_filter] = country_comparison

            # Update overall stats
            comparison_results["overall_comparison"]["api_total"] += len(api_products)
            comparison_results["overall_comparison"]["matched_by_id"] += country_comparison["matched_by_id"]
            comparison_results["overall_comparison"]["matched_by_name_country"] += country_comparison[
                "matched_by_name_country"
            ]
            comparison_results["overall_comparison"]["csv_only"] += country_comparison["csv_only"]
            comparison_results["overall_comparison"]["api_only"] += country_comparison["api_only"]
            comparison_results["overall_comparison"]["countries_processed"] += 1

        except Exception as e:
            self.logger.error(f"Failed to process country {country_filter}: {e}")
            comparison_results["country_comparisons"][country_filter] = {
                "error": str(e),
                "csv_count": len(csv_products_by_country[country_filter]),
                "api_count": 0,
            }

        # Calculate final statistics
        self._calculate_final_comparison_stats(comparison_results)

        # Save API data and comparison results
        if output_path:
            self._save_comprehensive_report(comparison_results, api_results, output_path)

        return comparison_results

    def _extract_product_data(self, row: Dict, row_num: int) -> Optional[Dict]:
        """Extract and validate product data from CSV row."""
        product_id = self._validate_and_format_uuid(row.get("id", "").strip())
        country_code = row.get("iso2_country_code", "").strip()

        if not product_id or not country_code:
            return None

        return {
            "id": product_id,
            "country_code": country_code,
            "country_name": row.get("country_name", "").strip(),
            "trade_name": row.get("trade_name", "").strip(),
            "registration_number": row.get("registration_number", "").strip(),
            "company": row.get("company", "").strip(),
            "entity_type": row.get("entity_type", "").strip(),
            "row_number": row_num,
        }

    def _analyze_csv(self, csv_path: str, country_filter: Optional[str] = None) -> Dict[str, Any]:
        """Analyze CSV file and group products by country."""
        filter_msg = f" (filtering for country: {country_filter})" if country_filter else ""
        self.logger.info(f"Analyzing CSV file: {csv_path}{filter_msg}")

        products_by_country = defaultdict(list)
        invalid_records = []
        total_products = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row_num, row in enumerate(reader, 1):
                product_data = self._extract_product_data(row, row_num)

                if not product_data:
                    invalid_records.append(
                        {
                            "row_number": row_num,
                            "id": row.get("id", ""),
                            "country_code": row.get("iso2_country_code", ""),
                            "reason": "Missing required fields",
                        }
                    )
                    continue

                # Apply country filter if specified
                if country_filter and product_data["country_code"].upper() != country_filter.upper():
                    continue

                products_by_country[product_data["country_code"]].append(product_data)
                total_products += 1

        result_msg = (
            f" for {country_filter}: {total_products} products"
            if country_filter
            else f": {total_products} products across {len(products_by_country)} countries"
        )
        self.logger.info(f"CSV analysis completed{result_msg}")

        return {
            "products_by_country": dict(products_by_country),
            "products": [prod for country_list in products_by_country.values() for prod in country_list],
            "invalid_records": invalid_records,
            "stats": {
                "total_products": total_products,
                "countries": list(products_by_country.keys()),
                "invalid_records": len(invalid_records),
            },
        }

    def _compare_country_data(
        self, csv_products: List[Dict[str, str]], api_products: List[Dict[str, Any]], country_code: str
    ) -> Dict[str, Any]:
        """Compare CSV products against API products for a specific country."""

        self.logger.debug(
            f"Comparing {len(csv_products)} CSV products vs {len(api_products)} API products for {country_code}"
        )

        # Create lookup sets for efficient matching
        api_products_by_id = {prod.get("id", "").lower(): prod for prod in api_products}
        api_products_by_name = defaultdict(list)

        for prod in api_products:
            name = prod.get("name", "").lower().strip()
            if name:
                api_products_by_name[name].append(prod)

        # Track matches and misses
        matched_by_id = []
        matched_by_name_country = []
        csv_only = []

        for csv_product in csv_products:
            csv_id = csv_product["id"].lower()
            csv_name = csv_product["trade_name"].lower().strip()

            # Try ID match first
            if csv_id in api_products_by_id:
                api_product = api_products_by_id[csv_id]
                matched_by_id.append(
                    {"csv_product": csv_product, "api_product": api_product, "match_method": "exact_id"}
                )

            # Try name match if no ID match
            elif csv_name and csv_name in api_products_by_name:
                # Find best name match for this country
                best_match = None
                for api_product in api_products_by_name[csv_name]:
                    if api_product.get("country", "").upper() == country_code.upper():
                        best_match = api_product
                        break

                if best_match:
                    matched_by_name_country.append(
                        {"csv_product": csv_product, "api_product": best_match, "match_method": "name_country"}
                    )
                else:
                    csv_only.append(csv_product)
            else:
                csv_only.append(csv_product)

        # Find API-only products (not in CSV)
        csv_ids = {prod["id"].lower() for prod in csv_products}
        csv_names = {prod["trade_name"].lower().strip() for prod in csv_products if prod["trade_name"].strip()}

        api_only = []
        for api_product in api_products:
            api_id = api_product.get("id", "").lower()
            api_name = api_product.get("name", "").lower().strip()

            if api_id not in csv_ids and api_name not in csv_names:
                api_only.append(api_product)

        comparison = {
            "country_code": country_code,
            "csv_count": len(csv_products),
            "api_count": len(api_products),
            "matched_by_id": len(matched_by_id),
            "matched_by_name_country": len(matched_by_name_country),
            "csv_only": len(csv_only),
            "api_only": len(api_only),
            "match_rate_by_id": (len(matched_by_id) / len(csv_products) * 100) if csv_products else 0,
            "total_match_rate": (
                ((len(matched_by_id) + len(matched_by_name_country)) / len(csv_products) * 100) if csv_products else 0
            ),
            "matches_by_id": matched_by_id,
            "matches_by_name_country": matched_by_name_country,
            "csv_only_products": csv_only,
            "api_only_products": api_only[:50],  # Limit for report size
        }

        self.logger.debug(
            f"Country {country_code} comparison: {comparison['match_rate_by_id']:.1f}% ID match, "
            f"{comparison['total_match_rate']:.1f}% total match"
        )

        return comparison

    def _calculate_final_comparison_stats(self, results: Dict[str, Any]):
        """Calculate final comparison statistics."""
        overall = results["overall_comparison"]

        if overall["csv_total"] > 0:
            overall["id_match_rate"] = (overall["matched_by_id"] / overall["csv_total"]) * 100
            overall["total_match_rate"] = (
                (overall["matched_by_id"] + overall["matched_by_name_country"]) / overall["csv_total"]
            ) * 100
        else:
            overall["id_match_rate"] = 0
            overall["total_match_rate"] = 0

        # Country-level summary
        country_summaries = []
        for country, comparison in results["country_comparisons"].items():
            if "error" not in comparison:
                country_summaries.append(
                    {
                        "country": country,
                        "csv_count": comparison["csv_count"],
                        "api_count": comparison["api_count"],
                        "id_matches": comparison["matched_by_id"],
                        "name_matches": comparison["matched_by_name_country"],
                        "total_matches": comparison["matched_by_id"] + comparison["matched_by_name_country"],
                        "match_rate": comparison["total_match_rate"],
                    }
                )

        # Sort by match rate
        country_summaries.sort(key=lambda x: x["match_rate"], reverse=True)
        results["country_summary"] = country_summaries

        # Add API statistics
        results["api_stats"] = {
            "total_products": overall["api_total"],
            "countries_with_data": len([c for c in results["country_comparisons"].values() if "error" not in c]),
            "countries_with_errors": len([c for c in results["country_comparisons"].values() if "error" in c]),
        }

    def _save_comprehensive_report(
        self, comparison_results: Dict[str, Any], api_results: Dict[str, List[Dict[str, Any]]], output_path: str
    ):
        """Save comprehensive comparison report and API data."""

        # Create output directory
        output_dir = os.path.dirname(output_path)
        if output_dir:
            FileManager.create_folder(output_dir)

        # Determine base name for related files
        base_name = os.path.splitext(output_path)[0]

        # Save main comparison report
        JSONManager.write_json(comparison_results, output_path)
        self.logger.info(f"Comparison report saved to: {output_path}")

        # Save raw API data
        api_data_path = f"{base_name}_api_products.json"
        JSONManager.write_json(api_results, api_data_path)
        self.logger.info(f"API products data saved to: {api_data_path}")

        # Save summary CSV
        summary_csv_path = f"{base_name}_summary.csv"
        self._save_summary_csv(comparison_results, summary_csv_path)
        self.logger.info(f"Summary CSV saved to: {summary_csv_path}")

    def _create_summary_row(self, country: str, comparison: Dict) -> Dict:
        """Create summary row for CSV export."""
        if "error" in comparison:
            return {
                "country_code": country,
                "csv_products": comparison.get("csv_count", 0),
                "api_products": comparison.get("api_count", 0),
                "id_matches": 0,
                "name_matches": 0,
                "total_matches": 0,
                "id_match_rate": 0,
                "total_match_rate": 0,
                "csv_only": 0,
                "api_only": 0,
                "status": f"ERROR: {comparison['error']}",
            }

        return {
            "country_code": country,
            "csv_products": comparison["csv_count"],
            "api_products": comparison["api_count"],
            "id_matches": comparison["matched_by_id"],
            "name_matches": comparison["matched_by_name_country"],
            "total_matches": comparison["matched_by_id"] + comparison["matched_by_name_country"],
            "id_match_rate": f"{comparison['match_rate_by_id']:.1f}%",
            "total_match_rate": f"{comparison['total_match_rate']:.1f}%",
            "csv_only": comparison["csv_only"],
            "api_only": comparison["api_only"],
            "status": "OK",
        }

    def _save_summary_csv(self, results: Dict[str, Any], csv_path: str):
        """Save a summary CSV with key statistics."""
        fieldnames = [
            "country_code",
            "csv_products",
            "api_products",
            "id_matches",
            "name_matches",
            "total_matches",
            "id_match_rate",
            "total_match_rate",
            "csv_only",
            "api_only",
            "status",
        ]

        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for country, comparison in results["country_comparisons"].items():
                writer.writerow(self._create_summary_row(country, comparison))

    def _validate_and_format_uuid(self, uuid_str: str) -> Optional[str]:
        """Validate and format UUID string."""
        try:
            clean_uuid = uuid_str.strip().lower()
            uuid_obj = uuid.UUID(clean_uuid)
            return str(uuid_obj)
        except (ValueError, AttributeError) as e:
            self.logger.warning(f"Invalid UUID format: '{uuid_str}' - {e}")
            return None
