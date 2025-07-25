"""
S3 Download Command
Downloads files from AWS S3 buckets with pagination and progress tracking.
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from domains.syngenta.aws.s3_download_service import S3DownloadService


class S3DownloadCommand(BaseCommand):
    """Command to download files from S3 buckets."""

    @staticmethod
    def get_name() -> str:
        return "s3-download"

    @staticmethod
    def get_description() -> str:
        return "Download files from AWS S3 bucket with advanced filtering and pagination support"

    @staticmethod
    def get_help() -> str:
        return """
Download all files from an AWS S3 bucket with the specified prefix.

FEATURES:
• Uses boto3 with default AWS CLI credentials
• Handles pagination for buckets with >1000 objects
• Preserves folder structure in local downloads
• Shows progress for each file
• Skips existing files with same size (configurable)
• Creates detailed download statistics
• Supports recursive/non-recursive downloads
• File extension filtering
• Maximum file count limiting

EXAMPLES:
  # Download all backup files from Cropwise catalog bucket
  python src/main.py syngenta aws s3-download \\
    --bucket cropwise.core.crops.catalog-prod-214187291705 \\
    --prefix backup/ \\
    --local-dir downloads

  # Download only JSON files, non-recursively, max 100 files
  python src/main.py syngenta aws s3-download \\
    --bucket my-bucket \\
    --prefix data/exports/ \\
    --local-dir /path/to/local/folder \\
    --no-recursive \\
    --max-files 100 \\
    --extensions .json .csv

  # Force re-download existing files
  python src/main.py syngenta aws s3-download \\
    --bucket my-bucket \\
    --prefix backup/ \\
    --no-skip-existing

REQUIREMENTS:
• AWS CLI configured with valid credentials
• Or AWS environment variables set (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
• boto3 Python package installed

The command will:
1. List all objects under the specified prefix (with pagination)
2. Download each file to the local directory
3. Preserve the S3 folder structure locally
4. Show progress and statistics
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        """Define command arguments."""
        parser.add_argument(
            "--bucket",
            required=True,
            help="S3 bucket name (e.g., cropwise.core.crops.catalog-prod-214187291705)",
        )
        parser.add_argument(
            "--prefix", required=True, help="S3 object prefix to filter by (e.g., backup/)"
        )
        parser.add_argument(
            "--local-dir",
            default="output/s3_downloads",
            help="Local directory to download files to (default: output/s3_downloads)",
        )
        parser.add_argument(
            "--no-recursive",
            action="store_true",
            help="Download only files at the current prefix level, not subdirectories",
        )
        parser.add_argument(
            "--max-files", type=int, help="Maximum number of files to download (default: unlimited)"
        )
        parser.add_argument(
            "--extensions",
            nargs="+",
            help="File extensions to filter by (e.g., --extensions .json .csv .txt)",
        )
        parser.add_argument(
            "--no-skip-existing",
            action="store_true",
            help="Re-download files even if they already exist locally",
        )

    @staticmethod
    def main(args: Namespace):
        """Execute the S3 download command."""
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("S3DownloadCommand")

        try:
            logger.info("Starting S3 download operation...")
            logger.info(f"Bucket: {args.bucket}")
            logger.info(f"Prefix: {args.prefix}")
            logger.info(f"Local directory: {args.local_dir}")
            logger.info(f"Recursive: {not args.no_recursive}")
            if args.max_files:
                logger.info(f"Max files: {args.max_files}")
            if args.extensions:
                logger.info(f"File extensions filter: {args.extensions}")
            logger.info(f"Skip existing files: {not args.no_skip_existing}")

            # Initialize service and execute download
            service = S3DownloadService()
            stats = service.download_all_files(
                bucket=args.bucket,
                prefix=args.prefix,
                local_dir=args.local_dir,
                recursive=not args.no_recursive,
                max_files=args.max_files,
                file_extensions=args.extensions,
                skip_existing=not args.no_skip_existing,
            )

            # Check results
            if stats["failed"] > 0:
                logger.warning(
                    f"Some downloads failed: {stats['failed']} out of {stats['total_files']}"
                )
                if stats["downloaded"] == 0:
                    logger.error("No files were successfully downloaded")
                    exit(1)

            logger.info("S3 download operation completed successfully")

        except Exception as e:
            logger.error(f"S3 download operation failed: {e}", exc_info=True)

            # Provide specific guidance for common issues
            if "Failed to initialize S3 client" in str(e):
                logger.error("")
                logger.error("🔧 TROUBLESHOOTING AWS CREDENTIALS:")
                logger.error("1. Install AWS CLI: brew install awscli (Mac) or pip install awscli")
                logger.error("2. Configure credentials: aws configure")
                logger.error("3. Test access: aws s3 ls")
                logger.error("4. Or set environment variables:")
                logger.error("   export AWS_ACCESS_KEY_ID=your_key")
                logger.error("   export AWS_SECRET_ACCESS_KEY=your_secret")
                logger.error("")

            exit(1)
