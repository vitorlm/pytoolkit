from argparse import ArgumentParser, Namespace

from utils.cache_manager.cache_manager import CacheManager
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

from .bulk_product_delete_service import BulkProductDeleteService
from .config import CatalogConfig


class BulkProductDeleteCommand(BaseCommand):
    """Command for bulk deleting products from CSV file using Cropwise Catalog API."""

    @staticmethod
    def get_name() -> str:
        return "bulk-product-delete"

    @staticmethod
    def get_description() -> str:
        return "Bulk delete products from CSV file using Cropwise Catalog API"

    @staticmethod
    def get_help() -> str:
        return """
        Bulk delete products from CSV file using Cropwise Catalog API.

        This command processes a CSV file containing product IDs and performs
        optimized bulk deletion operations with the following features:
        - Batch processing for improved performance
        - Detailed progress tracking and logging
        - Error handling with retry mechanism
        - Comprehensive deletion report generation
        - API rate limiting compliance

        CSV File Format:
        The CSV file should contain at least these columns:
        - country: Product country code (e.g., BR, AR, US)
        - product_id: UUID of the product to delete

        Examples:
        # Basic bulk deletion
        python src/main.py syngenta catalog bulk-product-delete \\
            --csv-path output/cw-catalog/ids-delete.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY

        # Delete products from specific country only
        python src/main.py syngenta catalog bulk-product-delete \\
            --csv-path output/cw-catalog/ids-delete.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --country BR

        # With custom batch size and dry run
        python src/main.py syngenta catalog bulk-product-delete \\
            --csv-path output/cw-catalog/ids-delete.csv \\
            --api-base-url https://api.cropwise.com \\
            --api-key YOUR_API_KEY \\
            --batch-size 50 \\
            --dry-run

        # With detailed logging and custom output directory
        python src/main.py syngenta catalog bulk-product-delete \\
            --csv-path output/cw-catalog/ids-delete.csv \\
            --output-dir output/bulk-delete-results \\
            --log-level DEBUG
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--csv-path",
            required=True,
            help="Path to CSV file containing product IDs to delete",
        )
        parser.add_argument(
            "--country",
            help="Filter products by country code (e.g., BR, US, IT). "
            "If not specified, all countries will be processed.",
        )
        parser.add_argument(
            "--api-base-url",
            default=CatalogConfig.CROPWISE_API_BASE_URL,
            help="Base URL for Cropwise API (default: from CROPWISE_API_BASE_URL env var)",
        )
        parser.add_argument(
            "--api-key",
            default=CatalogConfig.CROPWISE_API_KEY,
            help="API key for Cropwise authentication (default: from CROPWISE_API_KEY env var)",
        )
        parser.add_argument(
            "--batch-size",
            type=int,
            default=100,
            help="Number of products to process in each batch (default: 100)",
        )
        parser.add_argument(
            "--output-dir",
            default="output/bulk-delete-results",
            help="Output directory for reports and logs (default: output/bulk-delete-results)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Perform validation only without actual deletion",
        )
        parser.add_argument(
            "--skip-confirmation",
            action="store_true",
            help="Skip interactive confirmation prompt",
        )
        parser.add_argument(
            "--retry-attempts",
            type=int,
            default=3,
            help="Number of retry attempts for failed operations (default: 3)",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear all cached data before execution",
        )

    @staticmethod
    def main(args: Namespace):
        """Execute bulk product deletion command."""
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("BulkProductDeleteCommand")

        # Clear cache if requested
        if args.clear_cache:
            cache = CacheManager.get_instance()
            cache.clear_all()
            logger.info("Cache cleared successfully")
            print("✅ Cache cleared")

        # Validate API parameters
        if not args.api_base_url or not args.api_key:
            missing = []
            if not args.api_base_url:
                missing.append("API base URL")
            if not args.api_key:
                missing.append("API key")

            error_msg = f"Error: {' and '.join(missing)} required"
            logger.error(error_msg)
            print(error_msg)
            print("Use command line arguments or set environment variables")
            exit(1)

        try:
            logger.info("Starting bulk product deletion")
            logger.info(f"CSV file: {args.csv_path}")
            logger.info(
                f"Country filter: {args.country if args.country else 'All countries'}"
            )
            logger.info(f"API base URL: {args.api_base_url}")
            logger.info(f"Batch size: {args.batch_size}")
            logger.info(f"Output directory: {args.output_dir}")
            logger.info(f"Dry run: {args.dry_run}")
            logger.info(f"Skip confirmation: {args.skip_confirmation}")
            logger.info(f"Retry attempts: {args.retry_attempts}")

            # Initialize service with configuration
            service = BulkProductDeleteService()

            # Execute bulk deletion
            results = service.execute_bulk_delete(
                csv_path=args.csv_path,
                country_filter=args.country,
                api_base_url=args.api_base_url,
                api_key=args.api_key,
                batch_size=args.batch_size,
                output_dir=args.output_dir,
                retry_attempts=args.retry_attempts,
                dry_run=args.dry_run,
                skip_confirmation=args.skip_confirmation,
            )

            if results.get("success", False):
                logger.info("Bulk deletion completed successfully")
                logger.info(
                    f"Total products processed: {results.get('total_processed', 0)}"
                )
                logger.info(
                    f"Successful deletions: {results.get('successful_deletions', 0)}"
                )
                logger.info(f"Failed deletions: {results.get('failed_deletions', 0)}")
            else:
                error_msg = results.get("error", "Unknown error")
                logger.error(f"Bulk deletion failed: {error_msg}")
                exit(1)

        except Exception as e:
            logger.error(f"Bulk product deletion failed: {e}")
            exit(1)
