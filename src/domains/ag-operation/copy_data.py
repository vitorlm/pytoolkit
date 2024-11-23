# src/domains/ag-operation/copy_data.py
import logging
import os

from dotenv import load_dotenv

from utils.dynamodb_manager import DynamoDBManager

# Load environment variables from .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

# Create configuration dictionaries for source and target
source_config = {
    "aws_access_key_id": os.getenv("SOURCE_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("SOURCE_AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("SOURCE_AWS_REGION"),
}

target_config = {
    "aws_access_key_id": os.getenv("TARGET_AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("TARGET_AWS_SECRET_ACCESS_KEY"),
    "region_name": os.getenv("TARGET_AWS_REGION"),
}

# Initialize DynamoDBManager
manager = DynamoDBManager(source_config=source_config, target_config=target_config)

logger = logging.getLogger(__name__)

# Defining table names (adjust if they are different in each account)
AGRO_OPERATIONS_TABLE = os.getenv("AGRO_OPERATIONS_TABLE")
REVERSED_KEYS_TABLE = os.getenv("REVERSED_KEYS_TABLE")
PRODUCT_TYPE_TABLE = os.getenv("PRODUCT_TYPE_TABLE")
SUMMARIZED_AGRO_OP_TABLE = os.getenv("SUMMARIZED_AGRO_OP_TABLE")

if not all(
    [
        AGRO_OPERATIONS_TABLE,
        REVERSED_KEYS_TABLE,
        PRODUCT_TYPE_TABLE,
        SUMMARIZED_AGRO_OP_TABLE,
    ]
):
    raise EnvironmentError("One or more required environment variables are missing.")


def copy_agro_operations():
    """
    Copies the Agro Operations table and returns the operation IDs.
    """
    logger.info("Starting copy of Agro Operations table")
    return manager.copy_table_data(
        source_table_name=AGRO_OPERATIONS_TABLE, target_table_name=AGRO_OPERATIONS_TABLE
    )


def copy_reverse_keys(agro_operation_ids):
    """
    Copies data from the Reversed Keys table related to Agro Operations.
    """
    logger.info("Copying reversed keys related to Agro Operations")
    for operation_id in agro_operation_ids:
        manager.copy_related_data(
            source_table_name=REVERSED_KEYS_TABLE,
            target_table_name=REVERSED_KEYS_TABLE,
            foreign_key="operation_id",
            foreign_key_value=operation_id,
            index_name="agOpIdIndex",
        )


def copy_product_type_table():
    """
    Copies the Product Type table.
    """
    logger.info("Starting copy of Product Type table")
    manager.copy_table_data(
        source_table_name=PRODUCT_TYPE_TABLE, target_table_name=PRODUCT_TYPE_TABLE
    )


def copy_summarized_agro_operations():
    """
    Copies the Summarized Agro Operations table.
    """
    logger.info("Starting copy of Summarized Agro Operations table")
    manager.copy_table_data(
        source_table_name=SUMMARIZED_AGRO_OP_TABLE,
        target_table_name=SUMMARIZED_AGRO_OP_TABLE,
    )
