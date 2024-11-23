import logging
import time

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBManager:

    def __init__(self, source_config=None, target_config=None):
        """
        Initializes the DynamoDBCopier with source and target DynamoDB connections.

        Args:
            source_config (dict): Configuration for the source DynamoDB connection.
                Must contain 'aws_access_key_id', 'aws_secret_access_key', and 'region_name'.
            target_config (dict): Configuration for the target DynamoDB connection.
                Must contain 'aws_access_key_id', 'aws_secret_access_key', and 'region_name'.
        """
        if source_config is None and target_config is None:
            raise ValueError(
                "At least one of source_config or target_config must be provided."
            )

        if source_config is not None:
            self.source_dynamodb = boto3.resource("dynamodb", **source_config)
        if target_config is not None:
            self.target_dynamodb = boto3.resource("dynamodb", **target_config)

    def copy_table_data(self, source_table_name, target_table_name, limit=100):
        """
        Copies data from a source table to a target table in DynamoDB
        with pagination and retry mechanism for error handling.
        """
        logger.info(
            f"Copying data from table: {source_table_name} to {target_table_name}"
        )
        source_table = self.source_dynamodb.Table(source_table_name)
        target_table = self.target_dynamodb.Table(target_table_name)

        response = source_table.scan(Limit=limit)

        while True:
            items = response.get("Items", [])
            with target_table.batch_writer() as batch:
                for item in items:
                    self._retry_put_item(batch, item)

            if "LastEvaluatedKey" not in response:
                break

            response = source_table.scan(
                Limit=limit, ExclusiveStartKey=response["LastEvaluatedKey"]
            )

        logger.info(
            f"Data copied from table {source_table_name} to {target_table_name}"
        )

    def copy_related_data(
        self,
        source_table_name,
        target_table_name,
        foreign_key,
        foreign_key_value,
        index_name,
        limit=100,
    ):
        """
        Copies data from a source table to a target table based on a foreign key
        with pagination and retry mechanism for error handling.
        """
        logger.info(
            f"Copying related data from table: {source_table_name} to "
            f"{target_table_name} based on foreign key: {foreign_key}"
        )
        source_table = self.source_dynamodb.Table(source_table_name)
        target_table = self.target_dynamodb.Table(target_table_name)

        response = source_table.query(
            IndexName=index_name,
            KeyConditionExpression=boto3.dynamodb.conditions.Key(foreign_key).eq(
                foreign_key_value
            ),
            Limit=limit,
        )

        while True:
            items = response.get("Items", [])
            with target_table.batch_writer() as batch:
                for item in items:
                    self._retry_put_item(batch, item)

            if "LastEvaluatedKey" not in response:
                break

            response = source_table.query(
                IndexName=index_name,
                KeyConditionExpression=boto3.dynamodb.conditions.Key(foreign_key).eq(
                    foreign_key_value
                ),
                Limit=limit,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )

        logger.info(
            f"Related data copied from table {source_table_name} to {target_table_name}"
        )

    def _retry_put_item(self, batch, item):
        """
        Retries putting an item into a batch writer with exponential backoff.
        """
        retries = 0
        max_retries = 3
        while retries <= max_retries:
            try:
                batch.put_item(Item=item)
                logger.info(f"Copied item: {item}")
                break
            except ClientError as e:
                logger.error(f"Error putting item: {item}, Error: {e}")
                if retries == max_retries:
                    raise
                else:
                    sleep_time = 2**retries
                    logger.warning(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    retries += 1
