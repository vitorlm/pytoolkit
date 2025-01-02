from .config import Config
from utils.dynamodb_manager import DynamoDBManager
from utils.logging_manager import LogManager
from utils.base_command import BaseCommand
from .data_copy_processor import DataCopyProcessor
import argparse


# Configure logger
logger = LogManager.get_instance().get_logger("DataCopyProcessor")


class DataCopyCommand(BaseCommand):

    @staticmethod
    def get_arguments(parser: argparse.ArgumentParser) -> None:
        pass

    @staticmethod
    def main(args: argparse.Namespace) -> None:
        """
        Main function to orchestrate the copying of all required tables.
        """
        logger.info("Starting the data copy process")

        manager = DynamoDBManager()
        copy_processor = DataCopyProcessor(manager)

        # Ensure connections
        copy_processor.ensure_connection("source", Config.SOURCE_CONFIG)
        copy_processor.ensure_connection("target", Config.TARGET_CONFIG)

        # Copy Agro Operations table
        table_name = Config.AGRO_OPERATIONS_TABLE
        org_ids = [
            "069ad19f-dd28-4b3b-8bc4-30a5620b6cc7",
            "b8bf3cd9-853f-4455-875a-a4e6866855c8",
        ]

        copy_processor.ensure_table_exists_and_copy_structure(table_name)
        copy_processor.process_and_insert_operations(org_ids, table_name)

        logger.info("Data copy process completed successfully")
