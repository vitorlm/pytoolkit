import logging
from typing import List

from domains.syngenta.ag_operation.config import Config
from utils.dynamodb_manager import DynamoDBManager

# Initialize logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Initialize DynamoDBManager without any configurations
manager = DynamoDBManager()


def _ensure_connection(name: str, config: dict) -> None:
    """
    Ensures that a connection is configured and available.

    Args:
        name (str): Name of the connection (e.g., 'source', 'target').
        config (dict): Configuration for the connection.
    """
    if name not in manager.connection_configs:
        logger.info(f"Adding connection configuration for '{name}'.")
        manager.add_connection_config({"name": name, **config})


def _copy_table_if_not_exists(table_name: str) -> None:
    """
    Copy the table structure and data if the table doesn't exist in the target.

    Args:
        table_name (str): The name of the table to copy.
    """
    _ensure_connection("source", Config.SOURCE_CONFIG)
    _ensure_connection("target", Config.TARGET_CONFIG)

    if not manager.table_exists(table_name, "target"):
        logger.info(f"Table {table_name} does not exist. Creating...")
        manager.copy_table_structure("source", table_name, "target", table_name)
    manager.copy_table_data("source", table_name, "target", table_name)


def _get_operation_ids_batch() -> List[str]:
    """
    Retrieve all operation IDs from the Agro Operations table in batches.

    Returns:
        List[str]: List of operation IDs from the source table.
    """
    _ensure_connection("source", Config.SOURCE_CONFIG)

    table = manager.get_connection("source").Table(Config.AGRO_OPERATIONS_TABLE)
    response = table.scan(ProjectionExpression="operation_id")
    operation_ids = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="operation_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        operation_ids.extend(response.get("Items", []))

    return [item["operation_id"] for item in operation_ids]


def _copy_agro_operations() -> None:
    """
    Copies the Agro Operations table.
    """
    logger.info("Starting copy of Agro Operations table")
    _copy_table_if_not_exists(Config.AGRO_OPERATIONS_TABLE)


def _copy_reverse_keys() -> None:
    """
    Copies data from the Reversed Keys table related to Agro Operations.
    """
    logger.info("Copying reversed keys related to Agro Operations")
    _copy_table_if_not_exists(Config.REVERSED_KEYS_TABLE)

    operation_ids = _get_operation_ids_batch()
    for operation_id in operation_ids:
        _ensure_connection("source", Config.SOURCE_CONFIG)
        _ensure_connection("target", Config.TARGET_CONFIG)

        manager.copy_related_data(
            source_name="source",
            source_table_name=Config.REVERSED_KEYS_TABLE,
            target_name="target",
            target_table_name=Config.REVERSED_KEYS_TABLE,
            foreign_key="operation_id",
            foreign_key_value=operation_id,
            index_name="agOpIdIndex",
        )


def main(args=None):
    """
    Main function to orchestrate the copying of all the required tables.

    Args:
        args (Any, optional): Arguments passed to the script.
    """
    logger.info("Starting the data copy process")
    _copy_agro_operations()
    _copy_reverse_keys()
    logger.info("Data copy process completed successfully")


if __name__ == "__main__":
    main()
