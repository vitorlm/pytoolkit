"""
DynamoDB JSON Processor Command
Processes DynamoDB JSON export files and loads them into DuckDB.
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from domains.syngenta.aws.dynamodb_json_processor_service import DynamoDBJSONProcessorService


class DynamoDBJSONProcessorCommand(BaseCommand):
    """Command to process DynamoDB JSON exports and load into DuckDB."""

    @staticmethod
    def get_name() -> str:
        return "dynamodb-json-processor"

    @staticmethod
    def get_description() -> str:
        return "Process DynamoDB JSON export files and load them into DuckDB"

    @staticmethod
    def get_help() -> str:
        return """
Process DynamoDB JSON export files and load them into DuckDB.

This command handles two types of DynamoDB exports:

1. **AWS DynamoDB Exports** (RECOMMENDED):
   Automatically detects AWS export format by looking for:
   • manifest-summary.json - Contains export metadata
   • manifest-files.json - Lists all data files with expected item counts
   • data/ directory - Contains compressed .json.gz files with actual data

   Each data file contains JSON lines where each line has an "Item" object
   in DynamoDB format that gets converted to standard Python types.

2. **Generic JSON Files**:
   Falls back to processing individual .json files if AWS format not detected.

FEATURES:
• Automatically detects AWS DynamoDB export format vs generic JSON
• Reads export metadata for validation and progress tracking
• Processes compressed .json.gz files efficiently
• Converts DynamoDB types to Python native types
• TWO PROCESSING MODES:
  1. LEGACY: Merges all records into a single dataset
  2. STRUCTURED: Separates entities (PRODUCT, ITEM, etc.) into different tables
• Loads data into DuckDB for efficient analytics
• Progress tracking and comprehensive error handling
• Validates final record count against manifest
• Supports large datasets with memory-efficient batch processing
• Optional column mapping from DynamoDB names to custom DuckDB column names

STRUCTURED PROCESSING (--structured):
• Separates each entity type (rk field) into its own table:
  - PRODUCT → product_entities table
  - ITEM → item_entities table  
  - FERTILIZER → fertilizer_entities table
  - AUDIT → audit_entities table
  - VISIBILITY → visibility_entities table
• Maps DynamoDB field names to business-friendly column names:
  - pk → id, n → name, c → country, d → deleted, etc.
• Handles compressed binary fields (formulation, phrases, etc.)
• Creates business views (vw_product, vw_item, etc.) for easy querying
• Preserves unmapped columns with 'raw_' prefix for completeness

COLUMN MAPPING:
If a column mapping JSON file is provided via --column-mapping, the system will:
• Use mapped column names instead of auto-normalized names
• Apply data type transformations as specified in the mapping
• Handle compressed fields with automatic decompression
• Convert epoch timestamps to proper datetime format

Mapping file format (dynamo-to-duckdb-mapping.json):
{
  "columnMappings": {
    "_ct": {
      "targetName": "created_timestamp",
      "type": "timestamp",
      "transformation": "epoch_to_timestamp"
    },
    "n": {
      "targetName": "name",
      "type": "string"
    }
  }
}

DYNAMODB TYPE CONVERSIONS:
• S (String) → str
• N (Number) → int/float
• B (Binary) → bytes
• SS (String Set) → list[str]
• NS (Number Set) → list[int/float]
• BS (Binary Set) → list[bytes]
• M (Map) → dict
• L (List) → list
• NULL → None
• BOOL → bool

AWS EXPORT EXAMPLES:
  # Process AWS DynamoDB export (auto-detected) - Legacy single table
  python src/main.py syngenta aws dynamodb-json-processor \\
    --input-dir ./output/s3_downloads/AWSDynamoDB/01753445758221-fcc77707 \\
    --output-db catalog_export.duckdb \\
    --table-name products

  # RECOMMENDED: Process with structured entity separation
  python src/main.py syngenta aws dynamodb-json-processor \\
    --input-dir ./output/s3_downloads/AWSDynamoDB/01753445758221-fcc77707 \\
    --output-db catalog_structured.duckdb \\
    --structured \\
    --verbose

  # Structured processing without business views
  python src/main.py syngenta aws dynamodb-json-processor \\
    --input-dir ./output/s3_downloads/AWSDynamoDB/01753445758221-fcc77707 \\
    --output-db catalog_raw.duckdb \\
    --structured \\
    --no-create-views

  # Process with column mapping configuration (legacy mode)
  python src/main.py syngenta aws dynamodb-json-processor \\
    --input-dir ./output/s3_downloads/AWSDynamoDB/01753445758221-fcc77707 \\
    --output-db catalog_export.duckdb \\
    --table-name products \\
    --column-mapping ./src/domains/syngenta/aws/dynamo-to-duckdb-mapping.json

  # Process with custom settings
  python src/main.py syngenta aws dynamodb-json-processor \\
    --input-dir ./downloads/AWSDynamoDB/<ExportId> \\
    --output-db my_database.duckdb \\
    --table-name catalog_items \\
    --batch-size 5000 \\
    --verbose

GENERIC JSON EXAMPLES:
  # Process directory of regular JSON files
  python src/main.py syngenta aws dynamodb-json-processor \\
    --input-dir ./json_exports \\
    --output-db results.duckdb \\
    --table-name items \\
    --skip-empty-files

OUTPUT:
• Creates a DuckDB database file with the processed data
• Prints detailed progress during processing
• Shows summary statistics:
  - Files processed vs skipped
  - Total records imported vs manifest count
  - Number of errors with details
  - Final table record count and validation
• Logs detailed information for debugging and troubleshooting

AWS EXPORT STRUCTURE:
The input directory should contain:
├── manifest-summary.json    # Export metadata and item count
├── manifest-files.json      # List of data files with expected counts
├── data/                    # Directory containing compressed data files
│   ├── file1.json.gz       # Compressed JSON lines with "Item" objects
│   ├── file2.json.gz
│   └── ...

REQUIREMENTS:
• DuckDB, pandas, and related packages will be installed if not available
• Input directory must exist and contain valid files
• Sufficient disk space for the output database
• For AWS exports: manifest files and data directory must be present
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--input-dir", required=True, help="Directory containing DynamoDB JSON export files"
        )
        parser.add_argument(
            "--output-db",
            default="data/catalog_export.duckdb",
            help="Output DuckDB database file name (default: catalog_export.duckdb)",
        )
        parser.add_argument(
            "--table-name",
            default="products",
            help="Table name in DuckDB database (default: products)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=2500,
            help="Number of records to process in each batch (default: 2500)",
        )
        parser.add_argument(
            "--skip-empty-files",
            action="store_true",
            help="Skip empty or corrupted JSON files instead of failing",
        )
        parser.add_argument(
            "--column-mapping",
            help="Optional JSON file for column mapping configuration (DynamoDB to DuckDB)",
        )
        parser.add_argument(
            "--structured",
            action="store_true",
            help="Use structured processing: separate each entity type (PRODUCT, ITEM, etc.) into different tables with business-friendly column names",
        )
        parser.add_argument(
            "--no-create-views",
            action="store_true",
            help="Skip creating business-friendly views when using structured processing",
        )
        parser.add_argument("--verbose", action="store_true", help="Enable verbose progress output")

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("DynamoDBJSONProcessorCommand")

        try:
            # Validate table name for SQL compatibility
            import re

            if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", args.table_name):
                # If table name contains special characters, suggest sanitizing it
                sanitized_name = re.sub(r"[^a-zA-Z0-9_]", "_", args.table_name)
                logger.warning(
                    f"Table name '{args.table_name}' contains special characters. "
                    f"Using sanitized name: '{sanitized_name}'"
                )
                args.table_name = sanitized_name

            logger.info(f"Starting DynamoDB JSON processing from {args.input_dir}")

            service = DynamoDBJSONProcessorService()
            
            if args.structured:
                logger.info("Using structured processing - entities will be separated into different tables")
                result = service.process_exports_structured(
                    input_dir=args.input_dir,
                    output_db=args.output_db,
                    batch_size=args.batch_size,
                    skip_empty_files=args.skip_empty_files,
                    verbose=args.verbose,
                    create_views=not args.no_create_views,  # Create views by default, skip if --no-create-views
                )
            else:
                logger.info("Using legacy single-table processing")
                result = service.process_exports(
                    input_dir=args.input_dir,
                    output_db=args.output_db,
                    table_name=args.table_name,
                    batch_size=args.batch_size,
                    skip_empty_files=args.skip_empty_files,
                    verbose=args.verbose,
                    column_mapping_file=getattr(args, "column_mapping", None),
                )

            logger.info("DynamoDB JSON processing completed successfully")

            # Enhanced summary reporting
            logger.info("=" * 60)
            logger.info("PROCESSING SUMMARY")
            logger.info("=" * 60)
            
            if args.structured:
                # Structured processing results
                logger.info(f"Total records imported: {result['total_records']}")
                logger.info(f"Entity types found: {result['total_entities']}")
                logger.info(f"Tables created: {', '.join(result['tables_created'])}")
                logger.info(f"Views created: {result['views_created']}")
                
                logger.info("\nEntity Statistics:")
                for entity_type, count in result['entity_statistics'].items():
                    table_name = f"{entity_type.lower()}_entities"
                    logger.info(f"  • {entity_type}: {count:,} records → table '{table_name}'")
                
                # Show processing errors if any
                if result.get("error_count", 0) > 0:
                    logger.warning(f"\nProcessing errors encountered: {result['error_count']}")
                    for error_info in result.get("processing_errors", []):
                        logger.warning(f"  • File {error_info['file_index']}: {error_info['file']}")
                        logger.warning(f"    Error: {error_info['error']}")
                
            else:
                # Legacy processing results
                logger.info(f"Files processed: {result.get('files_processed', 0)}")
                logger.info(f"Files skipped: {result.get('files_skipped', 0)}")
                logger.info(f"Total records imported: {result.get('total_records', 0)}")
                logger.info(f"Errors encountered: {result.get('errors', 0)}")

                if result.get("final_table_count") is not None:
                    logger.info(f"Final table count: {result['final_table_count']}")

                if "manifest_item_count" in result:
                    manifest_count = result["manifest_item_count"]
                    logger.info(f"Manifest expected count: {manifest_count}")

                    if result.get("count_mismatch"):
                        logger.warning("⚠️  Record count mismatch detected!")
                    else:
                        logger.info("✅ Record count matches manifest")

                logger.info(f"Table name: {args.table_name}")

                # Error details if any
                if result.get("errors", 0) > 0:
                    logger.warning("Errors occurred during processing:")
                    for error_detail in result.get("error_details", []):
                        logger.warning(f"  • {error_detail['file']}: {error_detail['error']}")

            logger.info(f"Output database: {args.output_db}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"DynamoDB JSON processing failed: {e}", exc_info=True)
            exit(1)
