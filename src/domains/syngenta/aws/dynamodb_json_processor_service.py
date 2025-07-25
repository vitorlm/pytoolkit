"""
DynamoDB JSON Processor Service
Business logic for processing DynamoDB JSON exports and loading into DuckDB.
"""

import base64
import gzip
import json
import os
from typing import Any, Dict, List, Optional, cast

from utils.logging.logging_manager import LogManager
from utils.cache_manager.cache_manager import CacheManager
from utils.env_loader import ensure_env_loaded


class DynamoDBJSONProcessorService:
    """Service for processing DynamoDB JSON exports and loading into DuckDB."""

    def __init__(self):
        ensure_env_loaded()  # Load environment variables
        self.logger = LogManager.get_instance().get_logger("DynamoDBJSONProcessorService")
        self.cache = CacheManager.get_instance()
        self._column_mapping: Dict[str, str] = {}  # Original -> Normalized mapping
        self._column_conflicts: Dict[str, List[str]] = {}  # Normalized -> List of originals
        self._ensure_dependencies()

    def _normalize_column_name(self, column_name: str) -> str:
        """
        Normalize column name to be compatible with DuckDB.

        Strategy:
        1. Convert to lowercase for case-insensitive matching
        2. Replace special characters with underscores
        3. Ensure valid SQL identifier

        Args:
            column_name: Original column name from DynamoDB

        Returns:
            Normalized column name for DuckDB
        """
        import re

        # Convert to lowercase and replace special chars
        normalized = re.sub(r"[^a-zA-Z0-9_]", "_", column_name.lower())

        # Ensure it starts with letter or underscore
        if normalized and not normalized[0].isalpha() and normalized[0] != "_":
            normalized = f"col_{normalized}"

        # Remove multiple consecutive underscores
        normalized = re.sub(r"_+", "_", normalized)

        # Remove trailing underscores
        normalized = normalized.rstrip("_")

        return normalized or "unknown_col"

    def _build_column_mapping(self, all_columns: set[str]) -> tuple[Dict[str, str], Dict[str, str]]:
        """
        Build mapping between original and normalized column names.
        Handle conflicts when multiple original names normalize to the same name.

        Args:
            all_columns: Set of all original column names

        Returns:
            Tuple of (original_to_normalized, normalized_to_original) mappings
        """
        original_to_normalized: Dict[str, str] = {}
        normalized_to_original: Dict[str, str] = {}
        conflicts: Dict[str, List[str]] = {}

        # First pass: find all normalizations and conflicts
        for original in sorted(all_columns):
            normalized = self._normalize_column_name(original)

            if normalized in normalized_to_original:
                # Conflict detected
                if normalized not in conflicts:
                    # First conflict - add the existing mapping to conflicts
                    existing_original = normalized_to_original[normalized]
                    conflicts[normalized] = [existing_original]

                conflicts[normalized].append(original)
            else:
                normalized_to_original[normalized] = original
                original_to_normalized[original] = normalized

        # Second pass: resolve conflicts by adding suffixes
        for normalized, originals in conflicts.items():
            self.logger.warning(f"Column name conflict detected for '{normalized}': {originals}")

            # Remove the original mapping since we need to create new ones
            if normalized in normalized_to_original:
                original_without_suffix = normalized_to_original[normalized]
                if original_without_suffix in original_to_normalized:
                    del original_to_normalized[original_without_suffix]
                del normalized_to_original[normalized]

            # Create unique names with suffixes
            for i, original in enumerate(sorted(originals)):
                if i == 0:
                    # First one gets the base name
                    unique_normalized = normalized
                else:
                    # Others get numbered suffixes
                    unique_normalized = f"{normalized}_{i}"

                original_to_normalized[original] = unique_normalized
                normalized_to_original[unique_normalized] = original

                self.logger.info(f"Mapped '{original}' → '{unique_normalized}'")

        return original_to_normalized, normalized_to_original

    def _normalize_records(
        self, records: List[Dict[str, Any]], column_mapping: Dict[str, str]
    ) -> List[Dict[str, Any]]:
        """
        Normalize record column names using the provided mapping.

        Args:
            records: List of records with original column names
            column_mapping: Mapping from original to normalized column names

        Returns:
            List of records with normalized column names
        """
        normalized_records = []

        for record in records:
            normalized_record = {}
            for original_key, value in record.items():
                normalized_key = column_mapping.get(original_key, original_key)
                normalized_record[normalized_key] = value
            normalized_records.append(normalized_record)

        return normalized_records

    def _ensure_dependencies(self):
        """Install required packages if not available."""
        try:
            import duckdb  # noqa: F401
            import pandas as pd  # noqa: F401
        except ImportError as e:
            self.logger.info(f"Installing required packages: {e}")
            try:
                import subprocess
                import sys

                packages = ["duckdb", "pandas"]
                for package in packages:
                    try:
                        __import__(package)
                    except ImportError:
                        self.logger.info(f"Installing {package}...")
                        subprocess.check_call([sys.executable, "-m", "pip", "install", package])

                # Re-import after installation
                import duckdb  # noqa: F401
                import pandas as pd  # noqa: F401

                self.logger.info("All required packages installed successfully")

            except Exception as install_error:
                self.logger.error(f"Failed to install required packages: {install_error}")
                raise

    def process_exports(
        self,
        input_dir: str,
        output_db: str = "catalog_export.duckdb",
        table_name: str = "products",
        batch_size: int = 2500,
        skip_empty_files: bool = False,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Process AWS DynamoDB JSON export files and load into DuckDB.

        This method now handles AWS DynamoDB exports by:
        1. Reading manifest-summary.json and manifest-files.json
        2. Processing compressed .json.gz data files
        3. Extracting Item objects from DynamoDB format
        4. Converting to standard Python types and loading into DuckDB

        Args:
            input_dir: Directory containing AWS DynamoDB export
                (e.g., ./downloads/AWSDynamoDB/<ExportId>/)
            output_db: Output DuckDB database file name
            table_name: Table name in DuckDB
            batch_size: Number of records to process in each batch
            skip_empty_files: Skip empty/corrupted files instead of failing
            verbose: Enable verbose progress output

        Returns:
            Dictionary with processing summary
        """
        self.logger.info(f"Processing AWS DynamoDB exports from: {input_dir}")
        self.logger.info(f"Output database: {output_db}")
        self.logger.info(f"Table name: {table_name}")

        # Validate input directory
        if not os.path.exists(input_dir):
            raise ValueError(f"Input directory does not exist: {input_dir}")

        # Check if this is an AWS DynamoDB export directory
        is_aws_export = self._is_aws_dynamodb_export(input_dir)

        if is_aws_export:
            self.logger.info("Detected AWS DynamoDB export format")
            return self._process_aws_dynamodb_export(
                input_dir, output_db, table_name, batch_size, skip_empty_files, verbose
            )
        else:
            self.logger.info("Processing as generic JSON files")
            return self._process_generic_json_files(
                input_dir, output_db, table_name, batch_size, skip_empty_files, verbose
            )

    def _is_aws_dynamodb_export(self, input_dir: str) -> bool:
        """Check if directory contains AWS DynamoDB export files."""
        manifest_summary = os.path.join(input_dir, "manifest-summary.json")
        manifest_files = os.path.join(input_dir, "manifest-files.json")
        data_dir = os.path.join(input_dir, "data")

        return (
            os.path.exists(manifest_summary)
            and os.path.exists(manifest_files)
            and os.path.exists(data_dir)
        )

    def _process_aws_dynamodb_export(
        self,
        input_dir: str,
        output_db: str,
        table_name: str,
        batch_size: int,
        skip_empty_files: bool,
        verbose: bool,
    ) -> Dict[str, Any]:
        """Process AWS DynamoDB export using manifest files."""

        # Read manifest files
        manifest_summary_path = os.path.join(input_dir, "manifest-summary.json")
        manifest_files_path = os.path.join(input_dir, "manifest-files.json")
        data_dir = os.path.join(input_dir, "data")

        try:
            # Read manifest summary
            with open(manifest_summary_path, "r", encoding="utf-8") as f:
                manifest_summary = json.load(f)

            self.logger.info("Export summary:")
            self.logger.info(f"  Table: {manifest_summary.get('tableArn', 'Unknown')}")
            self.logger.info(f"  Export time: {manifest_summary.get('exportTime', 'Unknown')}")
            self.logger.info(f"  Total items: {manifest_summary.get('itemCount', 'Unknown')}")
            self.logger.info(f"  Output format: {manifest_summary.get('outputFormat', 'Unknown')}")

            # Read manifest files to get data file list
            data_files = []
            with open(manifest_files_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        file_info = json.loads(line)
                        data_files.append(file_info)

            self.logger.info(f"Found {len(data_files)} data files to process")

        except Exception as e:
            raise ValueError(f"Error reading manifest files: {e}")

        # Initialize statistics
        stats: Dict[str, Any] = {
            "files_processed": 0,
            "files_skipped": 0,
            "total_records": 0,
            "errors": 0,
            "error_details": [],
            "manifest_item_count": manifest_summary.get("itemCount", 0),
        }

        # Initialize DuckDB connection
        import duckdb

        conn = duckdb.connect(output_db)
        table_created = False

        self.logger.info(f"Processing {len(data_files)} data files with dynamic schema...")

        # Phase 1: Collect all column names to build comprehensive mapping
        self.logger.info("Phase 1: Analyzing all files to detect column schema...")
        all_columns: set[str] = set()

        for i, file_info in enumerate(data_files):
            try:
                s3_key = file_info["dataFileS3Key"]
                filename = os.path.basename(s3_key)
                file_path = os.path.join(data_dir, filename)

                if not os.path.exists(file_path):
                    continue

                if verbose:
                    self.logger.info(f"  Analyzing file {i + 1}/{len(data_files)}: {filename}")

                # Process file to get column names only
                records = self._process_aws_data_file(file_path, skip_empty_files)
                if records:
                    for record in records:
                        all_columns.update(record.keys())

            except Exception as e:
                self.logger.warning(
                    f"Error analyzing file {file_info.get('dataFileS3Key', 'unknown')}: {e}"
                )
                continue

        self.logger.info(f"Found {len(all_columns)} unique columns across all files")

        # Build column mapping to handle case sensitivity conflicts
        original_to_normalized, normalized_to_original = self._build_column_mapping(all_columns)

        self.logger.info(f"Column mapping created: {len(original_to_normalized)} mappings")
        if verbose:
            conflicts = sum(
                1
                for norm in normalized_to_original.keys()
                if "_" in norm and norm.split("_")[-1].isdigit()
            )
            if conflicts > 0:
                self.logger.info(f"Resolved {conflicts} column name conflicts")

        # Phase 2: Process files with normalized column names

        # Phase 2: Process files with normalized column names
        self.logger.info("Phase 2: Processing files with normalized schema...")

        # Process each file individually with normalized column names
        for i, file_info in enumerate(data_files):
            try:
                if verbose:
                    self.logger.info(f"Processing file {i + 1}/{len(data_files)}")
                    self.logger.info(f"  Expected items: {file_info.get('itemCount', 'Unknown')}")

                # Extract filename from S3 key
                s3_key = file_info["dataFileS3Key"]
                filename = os.path.basename(s3_key)
                file_path = os.path.join(data_dir, filename)

                if not os.path.exists(file_path):
                    error_msg = f"Data file not found: {file_path}"
                    self.logger.error(error_msg)
                    stats["errors"] = cast(int, stats["errors"]) + 1
                    error_details = cast(List[Dict[str, str]], stats["error_details"])
                    error_details.append({"file": file_path, "error": error_msg})
                    if not skip_empty_files:
                        raise FileNotFoundError(error_msg)
                    continue

                if verbose:
                    self.logger.info(f"  Processing: {file_path}")

                records = self._process_aws_data_file(file_path, skip_empty_files)

                if records:
                    # Normalize column names in records
                    normalized_records = self._normalize_records(records, original_to_normalized)

                    # Get normalized columns from current file
                    normalized_file_columns: set[str] = set()
                    for record in normalized_records:
                        normalized_file_columns.update(record.keys())

                    if verbose:
                        original_cols = len(set().union(*(record.keys() for record in records)))
                        normalized_cols = len(normalized_file_columns)
                        self.logger.info(
                            f"  → Normalized {original_cols} → {normalized_cols} columns"
                        )

                    # Adjust table schema if needed and load data with normalized columns
                    table_created = self._load_file_data_with_dynamic_schema(
                        conn,
                        normalized_records,
                        table_name,
                        table_created,
                        normalized_file_columns,
                        batch_size,
                        verbose,
                    )

                    stats["total_records"] = cast(int, stats["total_records"]) + len(records)
                    stats["files_processed"] = cast(int, stats["files_processed"]) + 1

                    if verbose:
                        self.logger.info(f"  → Extracted and loaded {len(records)} records")
                        expected_count = file_info.get("itemCount", 0)
                        if expected_count and len(records) != expected_count:
                            self.logger.warning(
                                f"  → Record count mismatch: got {len(records)}, "
                                f"expected {expected_count}"
                            )
                else:
                    stats["files_skipped"] = cast(int, stats["files_skipped"]) + 1
                    if verbose:
                        self.logger.info("  → No records found (empty file)")

            except Exception as e:
                file_key = file_info.get("dataFileS3Key", "unknown")
                error_msg = f"Error processing file {file_key}: {str(e)}"
                self.logger.error(error_msg)
                stats["errors"] = cast(int, stats["errors"]) + 1
                error_details = cast(List[Dict[str, str]], stats["error_details"])
                error_details.append({"file": file_key, "error": str(e)})

                if not skip_empty_files:
                    raise

        # Get final table statistics and validate against manifest
        try:
            result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
            final_count = result[0] if result else 0

            self.logger.info(f"Final table '{table_name}' contains {final_count} records")
            stats["final_table_count"] = final_count

            # Compare with manifest
            manifest_count = stats["manifest_item_count"]
            if manifest_count and final_count != manifest_count:
                self.logger.warning(
                    f"Record count mismatch with manifest: "
                    f"imported {final_count}, manifest says {manifest_count}"
                )
                stats["count_mismatch"] = True
            else:
                stats["count_mismatch"] = False

        except Exception as e:
            self.logger.warning(f"Could not get final table count: {e}")

        # Add column mapping information to stats
        stats["column_mapping"] = {
            "total_original_columns": len(all_columns),
            "total_normalized_columns": len(normalized_to_original),
            "mappings": original_to_normalized,
            "conflicts_resolved": len(
                [
                    k
                    for k in normalized_to_original.keys()
                    if "_" in k and k.split("_")[-1].isdigit()
                ]
            ),
        }

        if verbose and stats["column_mapping"]["conflicts_resolved"] > 0:
            self.logger.info(
                f"Column normalization summary: "
                f"{stats['column_mapping']['total_original_columns']} original → "
                f"{stats['column_mapping']['total_normalized_columns']} normalized columns, "
                f"{stats['column_mapping']['conflicts_resolved']} conflicts resolved"
            )

        conn.close()
        return stats

    def _process_generic_json_files(
        self,
        input_dir: str,
        output_db: str,
        table_name: str,
        batch_size: int,
        skip_empty_files: bool,
        verbose: bool,
    ) -> Dict[str, Any]:
        """Process generic JSON files (original behavior)."""

        # Find all JSON files
        json_files = self._find_json_files(input_dir)
        if not json_files:
            raise ValueError(f"No JSON files found in directory: {input_dir}")

        self.logger.info(f"Found {len(json_files)} JSON files to process")

        # Initialize statistics
        stats: Dict[str, Any] = {
            "files_processed": 0,
            "files_skipped": 0,
            "total_records": 0,
            "errors": 0,
            "error_details": [],
        }

        # Initialize DuckDB connection
        import duckdb

        conn = duckdb.connect(output_db)
        table_created = False

        self.logger.info(f"Processing {len(json_files)} JSON files with dynamic schema...")

        # Phase 1: Collect all column names to build comprehensive mapping
        self.logger.info("Phase 1: Analyzing all files to detect column schema...")
        all_columns: set[str] = set()

        for i, json_file in enumerate(json_files):
            try:
                if verbose:
                    file_short = json_file.replace(input_dir, "")
                    self.logger.info(f"  Analyzing file {i + 1}/{len(json_files)}: {file_short}")

                # Process file to get column names only
                records = self._process_json_file(json_file, skip_empty_files)
                if records:
                    for record in records:
                        all_columns.update(record.keys())

            except Exception as e:
                self.logger.warning(f"Error analyzing file {json_file}: {e}")
                continue

        self.logger.info(f"Found {len(all_columns)} unique columns across all files")

        # Build column mapping to handle case sensitivity conflicts
        original_to_normalized, normalized_to_original = self._build_column_mapping(all_columns)

        self.logger.info(f"Column mapping created: {len(original_to_normalized)} mappings")
        if verbose:
            conflicts = sum(
                1
                for norm in normalized_to_original.keys()
                if "_" in norm and norm.split("_")[-1].isdigit()
            )
            if conflicts > 0:
                self.logger.info(f"Resolved {conflicts} column name conflicts")

        # Phase 2: Process files with normalized column names
        self.logger.info("Phase 2: Processing files with normalized schema...")

        # Process each file individually with normalized column names
        for i, json_file in enumerate(json_files):
            try:
                if verbose:
                    file_short = json_file.replace(input_dir, "")
                    self.logger.info(f"Processing file {i + 1}/{len(json_files)}: {file_short}")

                records = self._process_json_file(json_file, skip_empty_files)

                if records:
                    # Normalize column names in records
                    normalized_records = self._normalize_records(records, original_to_normalized)

                    # Get normalized columns from current file
                    normalized_file_columns: set[str] = set()
                    for record in normalized_records:
                        normalized_file_columns.update(record.keys())

                    if verbose:
                        original_cols = len(set().union(*(record.keys() for record in records)))
                        normalized_cols = len(normalized_file_columns)
                        self.logger.info(f"  → Normalized {original_cols} → {normalized_cols} cols")

                    # Adjust table schema if needed and load data with normalized columns
                    table_created = self._load_file_data_with_dynamic_schema(
                        conn,
                        normalized_records,
                        table_name,
                        table_created,
                        normalized_file_columns,
                        batch_size,
                        verbose,
                    )

                    stats["total_records"] = cast(int, stats["total_records"]) + len(records)
                    stats["files_processed"] = cast(int, stats["files_processed"]) + 1

                    if verbose:
                        self.logger.info(f"  → Extracted and loaded {len(records)} records")
                else:
                    stats["files_skipped"] = cast(int, stats["files_skipped"]) + 1
                    if verbose:
                        self.logger.info("  → No records found (empty file)")

            except Exception as e:
                error_msg = f"Error processing file {json_file}: {str(e)}"
                self.logger.error(error_msg)
                stats["errors"] = cast(int, stats["errors"]) + 1
                error_details = cast(List[Dict[str, str]], stats["error_details"])
                error_details.append({"file": json_file, "error": str(e)})

                if not skip_empty_files:
                    raise

        # Get final table statistics
        try:
            result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
            final_count = result[0] if result else 0

            self.logger.info(f"Final table '{table_name}' contains {final_count} records")
            stats["final_table_count"] = final_count

        except Exception as e:
            self.logger.warning(f"Could not get final table count: {e}")

        # Add column mapping information to stats
        stats["column_mapping"] = {
            "total_original_columns": len(all_columns),
            "total_normalized_columns": len(normalized_to_original),
            "mappings": original_to_normalized,
            "conflicts_resolved": len(
                [
                    k
                    for k in normalized_to_original.keys()
                    if "_" in k and k.split("_")[-1].isdigit()
                ]
            ),
        }

        if verbose and stats["column_mapping"]["conflicts_resolved"] > 0:
            self.logger.info(
                f"Column normalization summary: "
                f"{stats['column_mapping']['total_original_columns']} original → "
                f"{stats['column_mapping']['total_normalized_columns']} normalized columns, "
                f"{stats['column_mapping']['conflicts_resolved']} conflicts resolved"
            )

        conn.close()

        return stats

    def _process_aws_data_file(
        self, file_path: str, skip_empty: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Process a single AWS DynamoDB export data file (.json.gz).

        Args:
            file_path: Path to the compressed JSON file
            skip_empty: Whether to skip empty files

        Returns:
            List of converted records
        """
        try:
            with gzip.open(file_path, "rt", encoding="utf-8") as f:
                content = f.read().strip()

            if not content:
                if skip_empty:
                    return []
                else:
                    raise ValueError("Empty file")

            # Parse JSON content line by line (each line is a separate JSON object)
            records = []

            for line_num, line in enumerate(content.split("\n"), 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)

                    # AWS DynamoDB export format has an "Item" key containing the actual record
                    if isinstance(data, dict) and "Item" in data:
                        converted = self._convert_dynamodb_item(data["Item"])
                        if converted:
                            records.append(converted)
                    else:
                        # Fallback: treat the entire object as a DynamoDB item
                        converted = self._convert_dynamodb_item(data)
                        if converted:
                            records.append(converted)

                except json.JSONDecodeError as e:
                    self.logger.warning(f"Invalid JSON on line {line_num} in {file_path}: {e}")
                    continue

            return records

        except Exception as e:
            raise ValueError(f"Error processing compressed file: {e}")

    def _find_json_files(self, directory: str) -> List[str]:
        """Recursively find all JSON files in a directory."""
        json_files = []

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.lower().endswith(".json"):
                    json_files.append(os.path.join(root, file))

        return sorted(json_files)

    def _process_json_file(self, file_path: str, skip_empty: bool = False) -> List[Dict[str, Any]]:
        """
        Process a single JSON file and extract DynamoDB items.

        Args:
            file_path: Path to the JSON file
            skip_empty: Whether to skip empty files

        Returns:
            List of converted records
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()

            if not content:
                if skip_empty:
                    return []
                else:
                    raise ValueError("Empty file")

            # Parse JSON content
            data = json.loads(content)

            # Handle different JSON structures
            records = []

            if isinstance(data, list):
                # Array of records
                for item in data:
                    converted = self._convert_dynamodb_item(item)
                    if converted:
                        records.append(converted)
            elif isinstance(data, dict):
                if "Item" in data:
                    # Single item with 'Item' key
                    converted = self._convert_dynamodb_item(data["Item"])
                    if converted:
                        records.append(converted)
                elif "Items" in data:
                    # Multiple items with 'Items' key
                    for item in data["Items"]:
                        converted = self._convert_dynamodb_item(item)
                        if converted:
                            records.append(converted)
                else:
                    # Assume the entire dict is a DynamoDB item
                    converted = self._convert_dynamodb_item(data)
                    if converted:
                        records.append(converted)

            return records

        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON format: {e}")
        except Exception as e:
            raise ValueError(f"Error processing file: {e}")

    def _convert_dynamodb_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Convert a DynamoDB item from DynamoDB JSON format to plain Python types.

        Args:
            item: DynamoDB item in JSON format

        Returns:
            Converted item as plain Python dict
        """
        if not isinstance(item, dict):
            return None

        converted = {}

        for key, value in item.items():
            converted[key] = self._convert_dynamodb_value(value)

        return converted

    def _convert_dynamodb_value(self, value: Any) -> Any:
        """
        Convert a DynamoDB value from DynamoDB JSON format to Python type.

        DynamoDB JSON format examples:
        - {"S": "hello"} → "hello"
        - {"N": "42"} → 42
        - {"N": "3.14"} → 3.14
        - {"B": "base64data"} → bytes
        - {"SS": ["a", "b"]} → ["a", "b"]
        - {"NS": ["1", "2"]} → [1, 2]
        - {"BS": ["data1", "data2"]} → [bytes, bytes]
        - {"M": {...}} → {...}
        - {"L": [...]} → [...]
        - {"NULL": true} → None
        - {"BOOL": true} → True
        """
        if not isinstance(value, dict):
            return value

        if len(value) != 1:
            # Not a DynamoDB typed value, return as-is
            return value

        type_key, type_value = next(iter(value.items()))

        if type_key == "S":  # String
            return str(type_value)
        elif type_key == "N":  # Number
            try:
                # Try integer first
                if "." not in str(type_value) and "e" not in str(type_value).lower():
                    return int(type_value)
                else:
                    return float(type_value)
            except (ValueError, TypeError):
                return type_value
        elif type_key == "B":  # Binary
            try:
                return base64.b64decode(type_value)
            except Exception:
                return type_value
        elif type_key == "SS":  # String Set
            return (
                [str(item) for item in type_value] if isinstance(type_value, list) else type_value
            )
        elif type_key == "NS":  # Number Set
            try:
                return [int(item) if "." not in str(item) else float(item) for item in type_value]
            except (ValueError, TypeError):
                return type_value
        elif type_key == "BS":  # Binary Set
            try:
                return [base64.b64decode(item) for item in type_value]
            except Exception:
                return type_value
        elif type_key == "M":  # Map
            if isinstance(type_value, dict):
                return {k: self._convert_dynamodb_value(v) for k, v in type_value.items()}
            return type_value
        elif type_key == "L":  # List
            if isinstance(type_value, list):
                return [self._convert_dynamodb_value(item) for item in type_value]
            return type_value
        elif type_key == "NULL":  # Null
            return None
        elif type_key == "BOOL":  # Boolean
            return bool(type_value)
        else:
            # Unknown type, return original value
            return value

    def _load_batch_to_duckdb(
        self, conn: Any, records: List[Dict[str, Any]], table_name: str, create_table: bool = False
    ) -> None:
        """
        Load a batch of records into DuckDB.

        Args:
            conn: DuckDB connection
            records: List of records to load
            table_name: Target table name
            create_table: Whether to create the table (first batch)
        """
        if not records:
            return

        import pandas as pd

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Handle complex data types by converting to JSON strings
        for col in df.columns:
            # Convert complex types (lists, dicts) to JSON strings
            if df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                )

        if create_table:
            # Create table and insert data
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM df')
        else:
            # Append to existing table
            conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM df')

    def _load_batch_to_duckdb_normalized(
        self,
        conn: Any,
        records: List[Dict[str, Any]],
        table_name: str,
        create_table: bool,
        all_columns: set[str],
    ) -> bool:
        """
        Load a batch of records into DuckDB with normalized schema.

        This method ensures all records have the same columns by:
        1. Normalizing all records to have the same column set
        2. Filling missing columns with None
        3. Creating or appending to table with consistent schema

        Args:
            conn: DuckDB connection
            records: List of records to load
            table_name: Target table name
            create_table: Whether to create the table (first batch)
            all_columns: Set of all possible column names

        Returns:
            True if table was created, False if appended
        """
        if not records:
            return False

        import pandas as pd

        # Normalize all records to have the same columns
        normalized_records = []
        sorted_columns = sorted(all_columns)  # Ensure consistent column order

        for record in records:
            normalized_record = {}
            for col in sorted_columns:
                normalized_record[col] = record.get(col, None)
            normalized_records.append(normalized_record)

        # Convert to DataFrame
        df = pd.DataFrame(normalized_records)

        # Handle complex data types by converting to JSON strings FIRST
        # This must be done before duplicate checking since lists are unhashable
        for col in df.columns:
            # Convert complex types (lists, dicts) to JSON strings
            if df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x
                )

        # Check for duplicates AFTER converting complex types
        original_count = len(df)
        if original_count > 0:
            try:
                # Check if we have potential primary key columns that might cause deduplication
                potential_pk_columns = [
                    col for col in df.columns if "id" in col.lower() or "key" in col.lower()
                ]

                if potential_pk_columns:
                    self.logger.debug(
                        f"Found potential primary key columns: {potential_pk_columns}"
                    )

                    # Check for duplicates on these columns
                    for pk_col in potential_pk_columns:
                        if pk_col in df.columns and not df[pk_col].isna().all():
                            try:
                                duplicate_count = df[pk_col].duplicated().sum()
                                if duplicate_count > 0:
                                    self.logger.warning(
                                        f"Found {duplicate_count} duplicates in column '{pk_col}'"
                                    )
                            except Exception as e:
                                self.logger.debug(
                                    f"Could not check duplicates for column '{pk_col}': {e}"
                                )

                # Check for completely duplicate rows
                duplicate_rows = df.duplicated().sum()
                if duplicate_rows > 0:
                    self.logger.warning(f"Found {duplicate_rows} completely duplicate rows")

            except Exception as e:
                self.logger.debug(f"Could not perform duplicate checking: {e}")

        if create_table:
            # Create table WITHOUT any implicit constraints by explicitly defining schema
            # This prevents DuckDB from auto-detecting primary keys based on column names

            # Build explicit column definitions
            column_definitions = []
            for col in sorted_columns:
                # Use a generic VARCHAR type for all columns to avoid any implicit constraints
                # We'll handle type inference later if needed
                safe_col_name = f'"{col}"'  # Quote column names to handle special characters
                column_definitions.append(f"{safe_col_name} VARCHAR")

            schema_sql = f'CREATE OR REPLACE TABLE "{table_name}" ({", ".join(column_definitions)})'

            self.logger.debug(
                f"Creating table with explicit schema: {len(column_definitions)} columns"
            )
            conn.execute(schema_sql)

            # Now insert the data
            conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM df')

            # Verify the table was created correctly
            try:
                result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
                actual_count = result[0] if result else 0
                if actual_count != len(df):
                    self.logger.error(
                        f"Initial table creation data mismatch! "
                        f"Inserted {len(df)} but table has {actual_count}"
                    )
            except Exception as e:
                self.logger.warning(f"Could not verify initial table creation: {e}")

            self.logger.info(
                f"Created table '{table_name}' with {len(sorted_columns)} columns "
                f"and {len(df)} rows"
            )
            return True
        else:
            # Check current table count before insert
            try:
                result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
                before_count = result[0] if result else 0
                self.logger.debug(f"Table count before insert: {before_count}")
            except Exception:
                before_count = 0

            # Append to existing table - use INSERT to avoid any UPSERT behavior
            conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM df')

            # Check count after insert to verify data was added
            try:
                result = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()
                after_count = result[0] if result else 0
                expected_count = before_count + len(df)

                self.logger.info(
                    f"Batch insert: {len(df)} records → "
                    f"Table count: {before_count} → {after_count} "
                    f"(expected: {expected_count})"
                )

                if after_count != expected_count:
                    missing_records = expected_count - after_count
                    self.logger.error(
                        f"DATA LOSS DETECTED! Missing {missing_records} records. "
                        f"This suggests DuckDB is deduplicating based on key columns."
                    )

                    # Check for potential causes
                    potential_pk_columns = [
                        col
                        for col in df.columns
                        if col.lower() in ["id", "pk", "key"] or "id" in col.lower()
                    ]
                    if potential_pk_columns:
                        self.logger.error(
                            f"Potential deduplication columns: {potential_pk_columns}"
                        )

                        # Sample some values to see if there are actual duplicates
                        for pk_col in potential_pk_columns[:3]:  # Check first 3 columns
                            if pk_col in df.columns:
                                sample_values = df[pk_col].value_counts().head(5)
                                sample_dict = sample_values.to_dict()
                                self.logger.debug(f"Sample {pk_col} values: {sample_dict}")

            except Exception as e:
                self.logger.warning(f"Could not verify insert: {e}")

            return False

    def _load_file_data_with_dynamic_schema(
        self,
        conn: Any,
        records: List[Dict[str, Any]],
        table_name: str,
        table_created: bool,
        file_columns: set[str],
        batch_size: int,
        verbose: bool = False,
    ) -> bool:
        """
        Load file data with dynamic schema adjustment.

        This method:
        1. Checks if table exists and gets current columns
        2. Compares file columns with table columns
        3. Adds missing columns to table if needed
        4. Processes data in batches and inserts into table

        Args:
            conn: DuckDB connection
            records: Records to load
            table_name: Target table name
            table_created: Whether table already exists
            file_columns: Set of columns in current file
            batch_size: Batch size for processing
            verbose: Enable verbose logging

        Returns:
            True indicating table exists after operation
        """
        import pandas as pd

        if not records:
            return table_created

        # Convert complex types to JSON strings first
        processed_records = []
        for record in records:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, (list, dict)):
                    processed_record[key] = json.dumps(value)
                else:
                    processed_record[key] = value
            processed_records.append(processed_record)

        if not table_created:
            # Create table with initial schema from first file
            if verbose:
                self.logger.info(f"Creating table '{table_name}' with {len(file_columns)} columns")

            # Create empty table first
            conn.execute(f'CREATE OR REPLACE TABLE "{table_name}" (temp_col VARCHAR)')

            # Add all columns explicitly as VARCHAR to avoid type conflicts
            for col in sorted(file_columns):
                safe_col_name = f'"{col}"'
                try:
                    conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN {safe_col_name} VARCHAR')
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        self.logger.warning(f"Could not add column {col}: {e}")

            # Remove temporary column
            try:
                conn.execute(f'ALTER TABLE "{table_name}" DROP COLUMN temp_col')
            except Exception:
                pass  # Ignore if temp column doesn't exist

            table_created = True
        else:
            # Table exists, check for missing columns
            try:
                # Get current table columns
                result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
                existing_columns = {row[0] for row in result}

                # Find missing columns
                missing_columns = file_columns - existing_columns

                if missing_columns:
                    if verbose:
                        missing_cols_list = sorted(missing_columns)
                        self.logger.info(
                            f"Adding {len(missing_columns)} new columns: {missing_cols_list}"
                        )

                    # Add missing columns
                    for col in sorted(missing_columns):
                        safe_col_name = f'"{col}"'
                        try:
                            conn.execute(
                                f'ALTER TABLE "{table_name}" ADD COLUMN {safe_col_name} VARCHAR'
                            )
                            if verbose:
                                self.logger.debug(f"Added column: {col}")
                        except Exception as e:
                            if "already exists" not in str(e).lower():
                                self.logger.warning(f"Could not add column {col}: {e}")

            except Exception as e:
                self.logger.warning(f"Could not check table schema: {e}")

        # Process records in batches
        for i in range(0, len(processed_records), batch_size):
            batch = processed_records[i : i + batch_size]

            try:
                # Create DataFrame for this batch
                df_batch = pd.DataFrame(batch)

                # Ensure all expected columns exist in DataFrame (fill with None if missing)
                table_result = conn.execute(f'DESCRIBE "{table_name}"').fetchall()
                table_columns = {row[0] for row in table_result}

                for col in table_columns:
                    if col not in df_batch.columns:
                        df_batch[col] = None

                # Reorder columns to match table schema
                df_batch = df_batch.reindex(columns=sorted(table_columns), fill_value=None)

                # Insert batch
                conn.execute(f'INSERT INTO "{table_name}" SELECT * FROM df_batch')

                if verbose:
                    self.logger.debug(f"Inserted batch of {len(batch)} records")

            except Exception as e:
                self.logger.error(f"Error inserting batch: {e}")
                raise

        return table_created
