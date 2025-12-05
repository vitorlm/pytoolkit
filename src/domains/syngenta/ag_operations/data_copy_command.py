from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.data.duckdb_manager import DuckDBManager
from utils.data.dynamodb_manager import DynamoDBManager
from utils.logging.logging_manager import LogManager

from .config import Config
from .data_copy_processor import DataCopyProcessor

# Configure logger
logger = LogManager.get_instance().get_logger("DataCopyCommand")


class DataCopyCommand(BaseCommand):
    """Command to handle data copy between DynamoDB tables for Agro Operations."""

    @staticmethod
    def get_name() -> str:
        return "data-copy"

    @staticmethod
    def get_description() -> str:
        return "Handles data copy between source and target DynamoDB tables for Agro Operations."

    @staticmethod
    def get_help() -> str:
        return (
            "This command connects to source and target DynamoDB tables, ensures table structures, "
            "and copies data for specified organization IDs."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--org-ids",
            type=str,
            nargs="+",
            required=False,
            help="List of organization IDs for which data will be copied.",
        )
        parser.add_argument(
            "--table-name",
            type=str,
            default=Config.AGRO_OPERATIONS_TABLE,
            help="Name of the DynamoDB table to copy data.",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        logger.info("Starting the data copy process")
        dynamodb_manager = DynamoDBManager()
        duckdb_manager = DuckDBManager()
        duckdb_manager.add_connection_config(
            {
                "name": "ag_operations_db",
                "path": "data/ag_operations.duckdb",
                "read_only": False,
            }
        )

        copy_processor = DataCopyProcessor(dynamodb_manager, duckdb_manager)

        logger.debug(f"Connecting to source: {Config.SOURCE_CONFIG}")
        copy_processor.ensure_connection("source", Config.SOURCE_CONFIG)

        table_name = args.table_name
        org_ids = args.org_ids

        if org_ids:
            logger.debug(f"Processing operations for org IDs: {org_ids}")
            copy_processor.process_and_insert_operations(table_name, org_ids)
        else:
            logger.debug("No org IDs provided, processing all operations")
            copy_processor.process_and_insert_operations_with_parquet(table_name)

        logger.info("Data copy process completed successfully")
