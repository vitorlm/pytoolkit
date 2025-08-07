"""
TUBE_HOMOLOGA Investigation Service

Service class that handles the business logic for investigating deleted
TUBE_HOMOLOGA products and their impact on our catalog database.
"""

import csv
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
import uuid
import duckdb
from utils.logging.logging_manager import LogManager
from utils.file_manager import FileManager
from utils.data.json_manager import JSONManager


class TubeHomologaInvestigationService:
    """Service for investigating deleted TUBE_HOMOLOGA products."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("TubeHomologaInvestigationService")

    def _validate_and_format_uuid(self, uuid_str: str) -> Optional[str]:
        """
        Validate and format UUID string to ensure it's in proper format.

        Args:
            uuid_str: String that should be a UUID

        Returns:
            Formatted UUID string if valid, None if invalid
        """
        try:
            # Remove any whitespace and convert to lowercase
            clean_uuid = uuid_str.strip().lower()

            # Try to parse as UUID - this validates the format
            uuid_obj = uuid.UUID(clean_uuid)

            # Return the standardized string representation
            return str(uuid_obj)

        except (ValueError, AttributeError) as e:
            self.logger.warning(f"Invalid UUID format: '{uuid_str}' - {e}")
            return None

    def investigate_deleted_products(
        self,
        csv_path: str,
        db_path: str,
        output_path: Optional[str] = None,
        summary_only: bool = False,
        batch_size: int = 1000,
        output_format: str = "json",
    ) -> Dict[str, Any]:
        """
        Investigate deleted TUBE_HOMOLOGA products and their impact.

        Args:
            csv_path: Path to CSV file with deleted products
            db_path: Path to DuckDB catalog database
            output_path: Optional path to save detailed report
            summary_only: If True, only return summary statistics
            batch_size: Batch size for database queries
            output_format: Output format for report ("json" or "csv")

        Returns:
            Dictionary with investigation results
        """
        # Validate output format
        if output_format not in ["json", "csv"]:
            raise ValueError(
                f"Unsupported output format: {output_format}. "
                f"Supported formats: json, csv"
            )

        self.logger.info("Starting deleted products investigation")

        # Validate inputs
        FileManager.validate_file(csv_path, allowed_extensions=[".csv"])
        FileManager.validate_file(db_path, allowed_extensions=[".duckdb"])

        # Read CSV data
        csv_result = self._read_csv_data(csv_path)
        deleted_products = csv_result["valid_products"]
        data_quality_info = csv_result["data_quality"]

        self.logger.info(f"Loaded {len(deleted_products)} valid products from CSV")

        # Connect to database and investigate
        with duckdb.connect(db_path, read_only=True) as conn:
            investigation_results = self._investigate_products(
                conn, deleted_products, batch_size, summary_only, data_quality_info
            )

        # Save detailed report if requested
        if output_path and not summary_only:
            if output_format == "csv":
                self._save_csv_report(investigation_results, output_path)
            else:
                self._save_detailed_report(investigation_results, output_path)

        return investigation_results

    def _read_csv_data(self, csv_path: str) -> Dict[str, Any]:
        """Read and parse CSV file with deleted products."""
        self.logger.info(f"Reading CSV file: {csv_path}")

        deleted_products = []
        invalid_uuids = []
        skipped_rows = 0

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            for row_num, row in enumerate(reader, 1):
                if len(row) >= 4:
                    # Validate and format the UUID
                    raw_pk = row[0].strip()
                    formatted_pk = self._validate_and_format_uuid(raw_pk)

                    if formatted_pk is not None:
                        deleted_products.append(
                            {
                                "pk": formatted_pk,
                                "entity_type": row[1].strip(),
                                "country_code": row[2].strip(),
                                "country_name": row[3].strip(),
                                "row_number": row_num,
                            }
                        )
                    else:
                        invalid_uuids.append(
                            {
                                "row_number": row_num,
                                "raw_pk": raw_pk,
                                "entity_type": row[1].strip(),
                                "country_code": row[2].strip(),
                            }
                        )
                        skipped_rows += 1
                else:
                    msg = f"Skipping row {row_num}: insufficient columns ({len(row)})"
                    self.logger.warning(msg)
                    skipped_rows += 1

        # Log summary of data quality
        self.logger.info("CSV processing completed:")
        self.logger.info(f"  - Valid products: {len(deleted_products)}")
        self.logger.info(f"  - Invalid UUIDs: {len(invalid_uuids)}")
        self.logger.info(f"  - Skipped rows: {skipped_rows}")

        if invalid_uuids:
            self.logger.warning(f"Found {len(invalid_uuids)} invalid UUIDs")
            # Log first few invalid UUIDs for debugging
            for invalid in invalid_uuids[:5]:
                self.logger.warning(
                    f"  Row {invalid['row_number']}: '{invalid['raw_pk']}' "
                    f"({invalid['country_code']})"
                )
            if len(invalid_uuids) > 5:
                self.logger.warning(f"  ... and {len(invalid_uuids) - 5} more")

        return {
            "valid_products": deleted_products,
            "data_quality": {
                "total_rows_read": len(deleted_products) + skipped_rows,
                "valid_products": len(deleted_products),
                "invalid_uuids": len(invalid_uuids),
                "skipped_rows": skipped_rows,
                "invalid_uuid_details": invalid_uuids[:10],  # Keep first 10 for reference
            },
        }

    def _detect_table_format(self, conn: duckdb.DuckDBPyConnection) -> Dict[str, str]:
        """
        Detect which table format is available in the database.

        Returns:
            Dict with table_name and format information
        """
        try:
            # Get list of available tables
            tables_result = conn.execute("SHOW TABLES").fetchall()
            available_tables = {row[0].lower() for row in tables_result}

            # Check for structured format tables (preferred)
            if "product_entities" in available_tables:
                self.logger.info("Using structured format: product_entities table")
                return {
                    "table_name": "product_entities",
                    "format": "structured",
                    "pk_column": "id",
                    "country_column": "country",
                    "entity_type_column": "table_name",
                    "source_column": "external_source",
                    "name_column": "name",
                    "deleted_column": "deleted",
                }
            elif "vw_product" in available_tables:
                self.logger.info("Using structured format: vw_product view")
                return {
                    "table_name": "vw_product",
                    "format": "structured",
                    "pk_column": "id",
                    "country_column": "country",
                    "entity_type_column": "table_name",
                    "source_column": "external_source",
                    "name_column": "name",
                    "deleted_column": "deleted",
                }
            elif "catalog_items" in available_tables:
                self.logger.info("Using legacy format: catalog_items table")
                return {
                    "table_name": "catalog_items",
                    "format": "legacy",
                    "pk_column": "pk",
                    "country_column": "c",
                    "entity_type_column": "_et",
                    "source_column": "exs",
                    "name_column": "n",
                    "deleted_column": "d",
                }
            else:
                # Default fallback
                self.logger.warning(
                    "No recognized table format found, using catalog_items as fallback"
                )
                return {
                    "table_name": "catalog_items",
                    "format": "fallback",
                    "pk_column": "pk",
                    "country_column": "c",
                    "entity_type_column": "_et",
                    "source_column": "exs",
                    "name_column": "n",
                    "deleted_column": "d",
                }

        except Exception as e:
            self.logger.error(f"Error detecting table format: {e}")
            # Safe fallback to legacy format
            return {
                "table_name": "catalog_items",
                "format": "error_fallback",
                "pk_column": "pk",
                "country_column": "c",
                "entity_type_column": "_et",
                "source_column": "exs",
                "name_column": "n",
                "deleted_column": "d",
            }

    def _investigate_products(
        self,
        conn: duckdb.DuckDBPyConnection,
        deleted_products: List[Dict[str, str]],
        batch_size: int,
        summary_only: bool,
        data_quality_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Investigate products against the database."""
        self.logger.info("Starting database investigation")

        # Detect table format and get column mappings
        table_config = self._detect_table_format(conn)

        results = {
            "csv_total": len(deleted_products),
            "found_in_db": 0,
            "not_found_in_db": 0,
            "match_rate": 0.0,
            "by_country": {},
            "by_entity_type": {},
            "found_products": [],
            "not_found_products": [],
            "data_quality": data_quality_info,
            "investigation_metadata": {
                "batch_size": batch_size,
                "summary_only": summary_only,
                "table_config": table_config,
            },
        }

        # Process batches by country
        unique_countries = list({product["country_code"] for product in deleted_products})
        total_batches = len(unique_countries)

        for batch_idx, country in enumerate(unique_countries):
            batch = [product for product in deleted_products if product["country_code"] == country]

            self.logger.info(
                f"Processing country batch {batch_idx + 1}/{total_batches} "
                f"for country '{country}' ({len(batch)} products)"
            )

            self._process_batch(conn, batch, results, summary_only, table_config)

        # Calculate final statistics
        self._calculate_final_statistics(results)

        self.logger.info("Investigation completed")
        return results

    def _process_batch(
        self,
        conn: duckdb.DuckDBPyConnection,
        batch: List[Dict[str, str]],
        results: Dict[str, Any],
        summary_only: bool,
        table_config: Dict[str, str],
    ):
        """Process a batch of deleted products."""
        # Create a list of PKs for batch query
        pk_list = [
            product["pk"] for product in batch
            if self._validate_and_format_uuid(product["pk"]) is not None
        ]
        pk_placeholders = ",".join(["?" for _ in pk_list])

        # Query database for matching products using detected table format
        pk_col = table_config["pk_column"]
        country_col = table_config["country_column"]
        entity_type_col = table_config["entity_type_column"]
        source_col = table_config["source_column"]
        name_col = table_config["name_column"]
        deleted_col = table_config["deleted_column"]
        table_name = table_config["table_name"]

        query = f"""
        SELECT
            {pk_col} AS pk,
            {country_col} AS country,
            {entity_type_col} AS entity_type,
            {source_col} AS source,
            {name_col} AS name,
            {deleted_col} AS deleted
        FROM {table_name}
        WHERE {pk_col} IN ({pk_placeholders})
        ORDER BY {country_col}
        """

        db_results = conn.execute(query, pk_list).fetchall()
        found_pks = {row[0] for row in db_results}

        # Process each product in the batch
        for product in batch:
            pk = product["pk"]
            country = product["country_code"]
            entity_type = product["entity_type"]

            # Initialize counters
            if country not in results["by_country"]:
                results["by_country"][country] = {"total": 0, "found": 0, "match_rate": 0.0}
            if entity_type not in results["by_entity_type"]:
                results["by_entity_type"][entity_type] = {"total": 0, "found": 0, "match_rate": 0.0}

            results["by_country"][country]["total"] += 1
            results["by_entity_type"][entity_type]["total"] += 1

            if pk in found_pks:
                # Found in database
                results["found_in_db"] += 1
                results["by_country"][country]["found"] += 1
                results["by_entity_type"][entity_type]["found"] += 1

                if not summary_only:
                    # Get detailed product info
                    db_product = next((row for row in db_results if row[0] == pk), None)
                    if db_product:
                        results["found_products"].append(
                            {
                                "csv_data": product,
                                "db_data": {
                                    "pk": db_product[0],
                                    "country": db_product[1],
                                    "entity_type": db_product[2],
                                    "source": db_product[3],
                                    "name": db_product[4],
                                    "deleted": db_product[5],
                                },
                            }
                        )
            else:
                # Not found in database
                results["not_found_in_db"] += 1
                if not summary_only:
                    results["not_found_products"].append(product)

    def _calculate_final_statistics(self, results: Dict[str, Any]):
        """Calculate final statistics and match rates."""
        total = results["csv_total"]
        found = results["found_in_db"]

        if total > 0:
            results["match_rate"] = (found / total) * 100

        # Calculate match rates by country
        for country_stats in results["by_country"].values():
            if country_stats["total"] > 0:
                rate = (country_stats["found"] / country_stats["total"]) * 100
                country_stats["match_rate"] = rate

        # Calculate match rates by entity type
        for entity_stats in results["by_entity_type"].values():
            if entity_stats["total"] > 0:
                rate = (entity_stats["found"] / entity_stats["total"]) * 100
                entity_stats["match_rate"] = rate

    def _save_detailed_report(self, results: Dict[str, Any], output_path: str):
        """Save detailed investigation report to JSON file."""
        self.logger.info(f"Saving detailed report to: {output_path}")

        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        if output_dir:
            FileManager.create_folder(output_dir)

        # Add metadata
        results["investigation_metadata"]["timestamp"] = datetime.now().isoformat()

        # Save to JSON
        JSONManager.write_json(results, output_path)

        self.logger.info("Detailed report saved successfully")

    def _save_csv_report(self, results: Dict[str, Any], output_path: str):
        """Save investigation report to CSV format."""
        self.logger.info(f"Saving CSV report to: {output_path}")

        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        if output_dir:
            FileManager.create_folder(output_dir)

        # Ensure output path has .csv extension
        if not output_path.endswith(".csv"):
            output_path = output_path.rsplit(".", 1)[0] + ".csv"

        # Create CSV report with found and not found products
        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "status",
                "csv_pk",
                "csv_entity_type",
                "csv_country_code",
                "csv_country_name",
                "csv_row_number",
                "db_pk",
                "db_country",
                "db_entity_type",
                "db_source",
                "db_name",
                "db_deleted",
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write found products
            for found_product in results.get("found_products", []):
                csv_data = found_product["csv_data"]
                db_data = found_product["db_data"]

                row = {
                    "status": "FOUND",
                    "csv_pk": csv_data["pk"],
                    "csv_entity_type": csv_data["entity_type"],
                    "csv_country_code": csv_data["country_code"],
                    "csv_country_name": csv_data["country_name"],
                    "csv_row_number": csv_data["row_number"],
                    "db_pk": db_data["pk"],
                    "db_country": db_data["country"],
                    "db_entity_type": db_data["entity_type"],
                    "db_source": db_data["source"],
                    "db_name": db_data["name"],
                    "db_deleted": db_data["deleted"],
                }
                writer.writerow(row)

            # Write not found products
            for not_found_product in results.get("not_found_products", []):
                row = {
                    "status": "NOT_FOUND",
                    "csv_pk": not_found_product["pk"],
                    "csv_entity_type": not_found_product["entity_type"],
                    "csv_country_code": not_found_product["country_code"],
                    "csv_country_name": not_found_product["country_name"],
                    "csv_row_number": not_found_product["row_number"],
                    "db_pk": "",
                    "db_country": "",
                    "db_entity_type": "",
                    "db_source": "",
                    "db_name": "",
                    "db_deleted": "",
                }
                writer.writerow(row)

        self.logger.info("CSV report saved successfully")

        # Also save a summary CSV file
        summary_path = output_path.replace(".csv", "_summary.csv")
        self._save_csv_summary(results, summary_path)

    def _save_csv_summary(self, results: Dict[str, Any], output_path: str):
        """Save investigation summary to CSV format."""
        self.logger.info(f"Saving CSV summary to: {output_path}")

        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write overall statistics
            writer.writerow(["OVERALL STATISTICS"])
            writer.writerow(["Metric", "Value"])
            writer.writerow(["Total CSV Products", results["csv_total"]])
            writer.writerow(["Found in DB", results["found_in_db"]])
            writer.writerow(["Not Found in DB", results["not_found_in_db"]])
            writer.writerow(["Match Rate (%)", f"{results['match_rate']:.2f}"])
            writer.writerow([])

            # Write data quality information
            writer.writerow(["DATA QUALITY STATISTICS"])
            writer.writerow(["Metric", "Value"])
            quality = results["data_quality"]
            writer.writerow(["Total Rows Read", quality["total_rows_read"]])
            writer.writerow(["Valid Products", quality["valid_products"]])
            writer.writerow(["Invalid UUIDs", quality["invalid_uuids"]])
            writer.writerow(["Skipped Rows", quality["skipped_rows"]])
            writer.writerow([])

            # Write statistics by country
            writer.writerow(["STATISTICS BY COUNTRY"])
            writer.writerow(["Country", "Total", "Found", "Match Rate (%)"])
            for country, stats in results["by_country"].items():
                writer.writerow(
                    [country, stats["total"], stats["found"], f"{stats['match_rate']:.2f}"]
                )
            writer.writerow([])

            # Write statistics by entity type
            writer.writerow(["STATISTICS BY ENTITY TYPE"])
            writer.writerow(["Entity Type", "Total", "Found", "Match Rate (%)"])
            for entity_type, stats in results["by_entity_type"].items():
                writer.writerow(
                    [entity_type, stats["total"], stats["found"], f"{stats['match_rate']:.2f}"]
                )

        self.logger.info("CSV summary saved successfully")
