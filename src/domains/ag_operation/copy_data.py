import logging
from functools import wraps
from typing import List

from botocore.exceptions import BotoCoreError, ClientError

from domains.ag_operation.config_copy_data import Config
from utils.dynamodb_manager import DynamoDBManager

# Initialize logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Initialize DynamoDBManager
manager = DynamoDBManager(
    source_config=Config.SOURCE_CONFIG, target_config=Config.TARGET_CONFIG
)


def _safe_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger.debug(f"Starting execution of function {func.__name__}")
            return func(*args, **kwargs)
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error occurred during {func.__name__}: {e}")

    return wrapper


def _copy_table_if_not_exists(table_name: str) -> None:
    """
    Copy the table structure and data if the table doesn't exist in the target.
    """
    if not manager.table_exists(table_name):
        logger.info(f"Table {table_name} does not exist. Creating...")
        manager.copy_table_structure(table_name, table_name)
    manager.copy_table_data(table_name, table_name)


@_safe_execution
def _get_operation_ids_batch() -> List[str]:
    """
    Retrieve all operation IDs from the Agro Operations table in batches.
    """
    table = manager.source_dynamodb.Table(Config.AGRO_OPERATIONS_TABLE)
    response = table.scan(ProjectionExpression="operation_id")
    operation_ids = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="operation_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        operation_ids.extend(response.get("Items", []))

    return [item["operation_id"] for item in operation_ids]


@_safe_execution
def _copy_agro_operations() -> None:
    """
    Copies the Agro Operations table.
    """
    logger.info("Starting copy of Agro Operations table")
    _copy_table_if_not_exists(Config.AGRO_OPERATIONS_TABLE)


@_safe_execution
def _copy_reverse_keys() -> None:
    """
    Copies data from the Reversed Keys table related to Agro Operations.
    """
    logger.info("Copying reversed keys related to Agro Operations")
    _copy_table_if_not_exists(Config.REVERSED_KEYS_TABLE)

    operation_ids = _get_operation_ids_batch()
    for operation_id in operation_ids:
        manager.copy_related_data(
            source_table_name=Config.REVERSED_KEYS_TABLE,
            target_table_name=Config.REVERSED_KEYS_TABLE,
            foreign_key="operation_id",
            foreign_key_value=operation_id,
            index_name="agOpIdIndex",
        )


@_safe_execution
def _copy_product_type_table() -> None:
    """
    Copies the Product Type table.
    """
    logger.info("Starting copy of Product Type table")
    _copy_table_if_not_exists(Config.PRODUCT_TYPE_TABLE)


@_safe_execution
def _copy_summarized_agro_operations() -> None:
    """
    Copies the Summarized Agro Operations table.
    """
    logger.info("Starting copy of Summarized Agro Operations table")
    _copy_table_if_not_exists(Config.SUMMARIZED_AGRO_OP_TABLE)


@_safe_execution
def main(args=None):
    """
    Main function to orchestrate the copying of all the required tables.
    """
    logger.info("Starting the data copy process")
    _copy_agro_operations()
    _copy_reverse_keys()
    _copy_product_type_table()
    _copy_summarized_agro_operations()
    logger.info("Data copy process completed successfully")


if __name__ == "__main__":
    main()
