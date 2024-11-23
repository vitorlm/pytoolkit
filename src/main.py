# Importing libraries
import logging
import os

import boto3

from utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

# AWS and DynamoDB configuration
AWS_REGION = "your-aws-region"
LOCAL_DYNAMODB_ENDPOINT = "http://localhost:8000"

# Initializing DynamoDB clients
aws_dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
local_dynamodb = boto3.resource("dynamodb", endpoint_url=LOCAL_DYNAMODB_ENDPOINT)

# Defining table names
AGRO_OPERATIONS_TABLE = os.getenv("AGRO_OPERATIONS_TABLE", "cws-agro-operations-devenv")
REVERSED_KEYS_TABLE = os.getenv(
    "REVERSED_KEYS_TABLE", "cws-agro-operations-reversed-keys-devenv"
)
PRODUCT_TYPE_TABLE = os.getenv("PRODUCT_TYPE_TABLE", "cws-product-type-devenv")
SUMMARIZED_AGRO_OP_TABLE = os.getenv(
    "SUMMARIZED_AGRO_OP_TABLE", "cws-summarized-agro-operations-devenv"
)


def migrate_table(source_table_name, target_table_name, limit=100):
    """
    Migrates data from a source table to a target table in DynamoDB.
    """
    logger.info(
        f"Migrating data from table: {source_table_name} to {target_table_name}"
    )
    source_table = aws_dynamodb.Table(source_table_name)
    target_table = local_dynamodb.Table(target_table_name)

    response = source_table.scan(Limit=limit)
    items = response.get("Items", [])

    with target_table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)
            logger.info(f"Migrated item: {item}")

    logger.info(
        f"{len(items)} items migrated from table {source_table_name} to {target_table_name}"
    )
    return items


def migrate_agro_operations():
    """
    Migrates the Agro Operations table and returns the operation IDs.
    """
    logger.info("Starting migration of Agro Operations table")
    return migrate_table(AGRO_OPERATIONS_TABLE, AGRO_OPERATIONS_TABLE)


def migrate_reverse_keys(agro_operation_ids):
    """
    Migrates data from the Reversed Keys table related to Agro Operations.
    """
    logger.info("Migrating reversed keys related to Agro Operations")
    source_table = aws_dynamodb.Table(REVERSED_KEYS_TABLE)
    target_table = local_dynamodb.Table(REVERSED_KEYS_TABLE)

    migrated_count = 0
    with target_table.batch_writer() as batch:
        for operation_id in agro_operation_ids:
            response = source_table.query(
                IndexName="agOpIdIndex",  # Name of the GSI, adjust as needed
                KeyConditionExpression=boto3.dynamodb.conditions.Key("operation_id").eq(
                    operation_id
                ),
            )
            items = response.get("Items", [])
            for item in items:
                batch.put_item(Item=item)
                migrated_count += 1
                logger.info(f"Migrated reversed key: {item}")

    logger.info(
        f"{migrated_count} reversed keys migrated from table {REVERSED_KEYS_TABLE}"
    )


def migrate_product_type_table():
    """
    Migrates the Product Type table.
    """
    logger.info("Starting migration of Product Type table")
    migrate_table(PRODUCT_TYPE_TABLE, PRODUCT_TYPE_TABLE)


def migrate_summarized_agro_operations():
    """
    Migrates the Summarized Agro Operations table.
    """
    logger.info("Starting migration of Summarized Agro Operations table")
    migrate_table(SUMMARIZED_AGRO_OP_TABLE, SUMMARIZED_AGRO_OP_TABLE)


def main():
    """
    Main function that coordinates the migration process of DynamoDB tables.
    """
    logger.info("Starting the migration process of all DynamoDB tables")
    agro_operations = migrate_agro_operations()
    agro_operation_ids = [
        item["operation_id"] for item in agro_operations if "operation_id" in item
    ]

    migrate_reverse_keys(agro_operation_ids)
    migrate_product_type_table()
    migrate_summarized_agro_operations()
    logger.info("Migration process completed")


if __name__ == "__main__":
    main()
