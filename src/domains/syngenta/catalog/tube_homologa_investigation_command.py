"""TUBE_HOMOLOGA Deleted Products Investigation Command

This command validates and investigates deleted TUBE_HOMOLOGA products by:
1. Reading a CSV file with deleted product information
2. Matching against our DuckDB catalog database
3. Providing impact analysis and reporting
"""

from argparse import ArgumentParser, Namespace

from utils.cache_manager.cache_manager import CacheManager
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class TubeHomologaInvestigationCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "tube-homologa-investigation"

    @staticmethod
    def get_description() -> str:
        return "Investigate deleted TUBE_HOMOLOGA products impact on our catalog"

    @staticmethod
    def get_help() -> str:
        return """
        Investigate deleted TUBE_HOMOLOGA products impact on our catalog.

        This command:
        1. Reads a CSV file containing deleted TUBE_HOMOLOGA products
        2. Matches them against our catalog database
        3. Provides impact analysis and reporting

        Examples:
        python src/main.py syngenta catalog tube-homologa-investigation \\
            --csv-path src/domains/syngenta/aws/tube-homologa-deleted-products-with-countries.csv \\
            --output-path output/tube_homologa_investigation_report.json

        python src/main.py syngenta catalog tube-homologa-investigation \\
            --csv-path src/domains/syngenta/aws/tube-homologa-deleted-products-with-countries.csv \\
            --output-path output/tube_homologa_investigation_report.csv \\
            --output-format csv

        python src/main.py syngenta catalog tube-homologa-investigation \\
            --csv-path src/domains/syngenta/aws/tube-homologa-deleted-products-with-countries.csv \\
            --summary-only
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--csv-path", required=True, help="Path to CSV file with deleted products")
        parser.add_argument(
            "--db-path",
            default="data/dynamodb_export.duckdb",
            help="Path to DuckDB catalog database (default: data/dynamodb_export.duckdb)",
        )
        parser.add_argument("--output-path", help="Path to save detailed report (optional)")
        parser.add_argument(
            "--output-format",
            choices=["json", "csv"],
            default="json",
            help="Output format for detailed report (default: json)",
        )
        parser.add_argument(
            "--summary-only",
            action="store_true",
            help="Show only summary statistics, no detailed listing",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear all cached data before execution",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("TubeHomologaInvestigationCommand")

        # Clear cache if requested
        if args.clear_cache:
            cache = CacheManager.get_instance()
            cache.clear_all()
            logger.info("Cache cleared successfully")
            print("✅ Cache cleared")

        try:
            logger.info("Starting TUBE_HOMOLOGA deleted products investigation")
            logger.info(f"CSV file: {args.csv_path}")
            logger.info(f"Database: {args.db_path}")

            # Import here to avoid circular import issues
            from .tube_homologa_investigation_service import (
                TubeHomologaInvestigationService,
            )

            service = TubeHomologaInvestigationService()
            result = service.investigate_deleted_products(
                csv_path=args.csv_path,
                db_path=args.db_path,
                output_path=args.output_path,
                summary_only=args.summary_only,
                output_format=args.output_format,
            )

            logger.info("Investigation completed successfully")

            # Print summary to console
            print("\n" + "=" * 60)
            print("TUBE_HOMOLOGA DELETED PRODUCTS INVESTIGATION SUMMARY")
            print("=" * 60)

            # Data quality information
            if "data_quality" in result:
                dq = result["data_quality"]
                print("Data Quality:")
                print(f"  Total rows processed: {dq.get('total_rows_read', 'N/A')}")
                print(f"  Valid UUIDs: {dq.get('valid_products', 'N/A')}")
                print(f"  Invalid UUIDs skipped: {dq.get('invalid_uuids', 0)}")
                if dq.get("invalid_uuids", 0) > 0:
                    print(f"  Skipped rows: {dq.get('skipped_rows', 0)}")
                print()

            print("Investigation Results:")
            print(f"  Products in CSV (valid): {result['csv_total']}")
            print(f"  Products found in database: {result['found_in_db']}")
            print(f"  Products NOT found in database: {result['not_found_in_db']}")
            print(f"  Match rate: {result['match_rate']:.1f}%")

            if result["by_country"]:
                print("\nBreakdown by Country:")
                for country, stats in result["by_country"].items():
                    found = stats["found"]
                    total = stats["total"]
                    rate = stats["match_rate"]
                    print(f"  {country}: {found}/{total} found ({rate:.1f}%)")

            if result["by_entity_type"]:
                print("\nBreakdown by Entity Type:")
                for entity_type, stats in result["by_entity_type"].items():
                    found = stats["found"]
                    total = stats["total"]
                    rate = stats["match_rate"]
                    print(f"  {entity_type}: {found}/{total} found ({rate:.1f}%)")

            if args.output_path:
                format_info = f" ({args.output_format} format)"
                if args.output_format == "csv":
                    print(f"\nDetailed report saved to: {args.output_path}{format_info}")
                    summary_path = args.output_path.replace(".csv", "_summary.csv")
                    print(f"Summary report saved to: {summary_path}")
                else:
                    print(f"\nDetailed report saved to: {args.output_path}{format_info}")

        except Exception as e:
            logger.error(f"Investigation failed: {e}", exc_info=True)
            print(f"Error: {e}")
            exit(1)
