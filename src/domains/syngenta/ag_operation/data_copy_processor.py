from utils.data.dynamodb_manager import DynamoDBManager, CompositeKey
from utils.logging.logging_manager import LogManager
from typing import List

# Configure logger
logger = LogManager.get_instance().get_logger("DataCopyProcessor")


class DataCopyProcessor:
    def __init__(self, manager: DynamoDBManager):
        self.manager = manager

    def ensure_connection(self, name: str, config: dict) -> None:
        """
        Ensure a DynamoDB connection is configured.
        """
        if name not in self.manager.connection_configs:
            logger.info(f"Adding connection configuration for '{name}'.")
            self.manager.add_connection_config({"name": name, **config})

    def ensure_table_exists_and_copy_structure(self, table_name: str) -> None:
        """
        Ensure a table exists in the target and copy its structure if necessary.
        """
        if not self.manager.table_exists(table_name, "target"):
            logger.info(f"Table {table_name} does not exist. Creating...")
            self.manager.copy_table_structure("source", table_name, "target", table_name)

    def copy_table_data(self, table_name: str, limit: int) -> None:
        """
        Copy data from the source table to the target table.
        """
        logger.info(f"Starting copy of {table_name} table")
        self.ensure_table_exists_and_copy_structure(table_name)
        self.manager.copy_table_data("source", table_name, "target", table_name, limit)

    def process_and_insert_operations(self, org_ids: List[str], table_name: str) -> None:
        """
        Process and insert operations for a list of organization IDs.
        """
        logger.info("Processing operations and summaries...")
        sum_pks = [CompositeKey.create(org_id, "SUM") for org_id in org_ids]
        pks = sum_pks + org_ids
        operation_records = self.manager.query_partition_keys_parallel("source", table_name, pks)
        logger.info(f"Retrieved {len(operation_records)} operations and summaries.")

        logger.info("Inserting operations and summaries into target.")
        self.manager.insert_records_with_retries("target", table_name, operation_records)
        logger.info("Operations and summaries inserted successfully.")

        logger.info("Processing work orders and records...")
        work = []
        for operation in operation_records:
            if not operation["pk"].endswith("#SUM"):
                ag_operation_id = operation["sf_ago_id"]
                logger.info(f"Processing work orders and records for org_id: {ag_operation_id}")
                suffixes = ["WR", "WO"]
                prefixes = ["", "DEL#"]
                # Generate partition keys
                for prefix in prefixes:
                    for suffix in suffixes:
                        work.append(CompositeKey.create(prefix, ag_operation_id, suffix))
        logger.info(f"Retrieved {len(work)} work orders and records.")
        work_records = self.manager.query_partition_keys_parallel("source", table_name, work)
        logger.info("Inserting work orders and records into target.")
        self.manager.insert_records_with_retries("target", table_name, work_records)
        logger.info("Work orders and records inserted successfully.")
