from utils.data.duckdb_manager import DuckDBManager
from utils.data.dynamodb_manager import CompositeKey, DynamoDBManager
from utils.logging.logging_manager import LogManager

# Configure logger
logger = LogManager.get_instance().get_logger("DataCopyProcessor")


class DataCopyProcessor:
    def __init__(self, dynamodb_manager: DynamoDBManager, duckdb_manager: DuckDBManager):
        self._dynamodb_manager = dynamodb_manager
        self._duckdb_manager = duckdb_manager

    def ensure_connection(self, name: str, config: dict) -> None:
        """Ensure a DynamoDB connection is configured."""
        if name not in self._dynamodb_manager.connection_configs:
            logger.info(f"Adding connection configuration for '{name}'.")
            self._dynamodb_manager.add_connection_config({"name": name, **config})

    def ensure_table_exists_and_copy_structure(self, table_name: str) -> None:
        """Ensure a table exists in the target and copy its structure if necessary."""
        if not self._dynamodb_manager.table_exists(table_name, "target"):
            logger.info(f"Table {table_name} does not exist. Creating...")
            self._dynamodb_manager.copy_table_structure("source", table_name, "target", table_name)

    def process_and_insert_operations(self, table_name: str, org_ids: list[str] | None = None) -> None:
        """Process and insert operations for a list of organization IDs."""
        if org_ids:
            logger.info("Processing operations and summaries...")
            sum_pks = [CompositeKey.create(org_id, "SUM") for org_id in org_ids]
            pks = sum_pks + org_ids
            operation_records = self._dynamodb_manager.query_partition_keys_parallel("source", table_name, pks)
            logger.info(f"Retrieved {len(operation_records)} operations and summaries.")
        else:
            logger.info("Processing all operations and summaries...")
            operation_records = self._dynamodb_manager.get_data_with_filter("source", table_name=table_name)
            logger.info(f"Retrieved {len(operation_records)} operations and summaries.")

        logger.info("Processing work orders and records...")
        operations = []
        works = []
        for operation in operation_records:
            ag_operation_id = operation.get("sf_ago_id")
            pk = operation.get("pk")
            if not ag_operation_id:
                logger.warning(f"sf_ago_id not found in operation: {operation}")
                continue
            elif pk and (pk.endswith("WR") or pk.endswith("WO")):
                works.append(operation)
            else:
                operations.append(operation)

        operations_schema = self._duckdb_manager.create_table("ag_operations_db", "operations", sample_data=operations)
        self._duckdb_manager.insert_records("ag_operations_db", "operations", operations_schema, operations)
        logger.info(f"Inserted {len(operations)} operations.")
        works_schema = self._duckdb_manager.create_table("ag_operations_db", "works", sample_data=works)
        self._duckdb_manager.insert_records("ag_operations_db", "works", works_schema, works)
        logger.info(f"Inserted {len(works)} work orders and records.")
        logger.info("Data copy and processing complete.")

    def process_and_insert_operations_with_parquet(self, table_name: str, org_ids: list[str] | None = None) -> None:
        """Process operations and work orders from DynamoDB by scanning the table incrementally
        into a Parquet file, then processing the Parquet file and inserting records into DuckDB.

        If org_ids is provided, only operations for those organizations (and their summaries)
        are processed using a filter expression. Otherwise, the entire table is scanned.

        This method uses scan_dynamodb_to_parquet_with_checkpoint to avoid holding all
        data in memory, and to store progress so that the scan can be resumed in case of a failure.
        """
        # --- Step 1: Build a filter expression if org_ids are provided ---
        logger.info("Processing all operations and summaries (full table scan).")
        filter_expression = None

        # --- Step 2: Perform an incremental scan and create a Parquet file ---
        output_parquet_path = "data/operations.parquet"
        checkpoint_path = "data/operations_checkpoint.json"
        self._dynamodb_manager.scan_dynamodb_to_parquet_with_checkpoint(
            connection_name="source",
            table_name=table_name,
            filter_expression=filter_expression,
            limit=None,
            max_workers=20,
            output_parquet_path=output_parquet_path,
            checkpoint_path=checkpoint_path,
            scan_batch_limit=2500,
        )
        logger.info(f"Parquet file created at {output_parquet_path}.")

        # --- Step 3: Load the Parquet file and process the records ---
        try:
            import pyarrow.parquet as pq

            table = pq.read_table(output_parquet_path)
            records = table.to_pylist()
            logger.info(f"Loaded {len(records)} records from the Parquet file.")
        except Exception as e:
            logger.error(f"Error reading Parquet file: {e}")
            return

        # Separate records into operations and work orders (or records)
        operations = []
        works = []
        for record in records:
            ag_operation_id = record.get("sf_ago_id")
            pk = record.get("pk")
            if not ag_operation_id:
                logger.warning(f"sf_ago_id not found in record: {record}")
                continue
            if pk and (pk.endswith("WR") or pk.endswith("WO")):
                works.append(record)
            else:
                operations.append(record)

        logger.info(f"Separated records into {len(operations)} operations and {len(works)} work orders/records.")

        # --- Step 4: Create DuckDB tables and insert the processed records ---
        operations_schema = self._duckdb_manager.create_table("ag_operations_db", "operations", sample_data=operations)
        self._duckdb_manager.insert_records("ag_operations_db", "operations", operations_schema, operations)
        logger.info(f"Inserted {len(operations)} operations into DuckDB.")

        works_schema = self._duckdb_manager.create_table("ag_operations_db", "works", sample_data=works)
        self._duckdb_manager.insert_records("ag_operations_db", "works", works_schema, works)
        logger.info(f"Inserted {len(works)} work orders and records into DuckDB.")
