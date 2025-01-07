from argparse import ArgumentParser, Namespace
from utils.base_command import BaseCommand
from .data_copy_processor import DataCopyProcessor
from utils.dynamodb_manager import DynamoDBManager
from log_config import LogManager
from .config import Config

# Configure logger
logger = LogManager.get_instance().get_logger("DataCopyCommand")


class DataCopyCommand(BaseCommand):
    """
    Command to handle data copy between DynamoDB tables for Agro Operations.
    """

    @staticmethod
    def get_name() -> str:
        return "data_copy"

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
            required=True,
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
        manager = DynamoDBManager()
        copy_processor = DataCopyProcessor(manager)

        logger.debug(f"Connecting to source: {Config.SOURCE_CONFIG}")
        copy_processor.ensure_connection("source", Config.SOURCE_CONFIG)

        logger.debug(f"Connecting to target: {Config.TARGET_CONFIG}")
        copy_processor.ensure_connection("target", Config.TARGET_CONFIG)

        table_name = args.table_name
        org_ids = args.org_ids

        logger.debug(f"Ensuring table structure for: {table_name}")
        copy_processor.ensure_table_exists_and_copy_structure(table_name)

        logger.debug(f"Processing operations for org IDs: {org_ids}")
        copy_processor.process_and_insert_operations(org_ids, table_name)

        logger.info("Data copy process completed successfully")
