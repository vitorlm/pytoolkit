import logging

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


class DynamoDBManager:
    def __init__(self, source_config=None, target_config=None):
        """
        Initializes the DynamoDBManager with source and target DynamoDB connections.

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

        if source_config:
            self.source_dynamodb = boto3.resource("dynamodb", **source_config)
        if target_config:
            self.target_dynamodb = boto3.resource("dynamodb", **target_config)

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def copy_table_data(self, source_table_name, target_table_name, limit=100):
        """
        Copies data from a source table to a target table in DynamoDB with pagination.

        Args:
            source_table_name (str): The name of the source table.
            target_table_name (str): The name of the target table.
            limit (int): Maximum number of items to scan at a time.
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

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
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
        Copies related data from a source table to a target table based on a foreign key.

        Args:
            source_table_name (str): The name of the source table.
            target_table_name (str): The name of the target table.
            foreign_key (str): The foreign key to filter items.
            foreign_key_value (str): The value of the foreign key to filter items.
            index_name (str): The index name to use for the query.
            limit (int): Maximum number of items to query at a time.
        """
        logger.info(
            f"Copying related data from {source_table_name} to {target_table_name} based on foreign key: {foreign_key}"
        )

        source_table = self.source_dynamodb.Table(source_table_name)
        target_table = self.target_dynamodb.Table(target_table_name)

        response = source_table.query(
            IndexName=index_name,
            KeyConditionExpression=Key(foreign_key).eq(foreign_key_value),
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
                KeyConditionExpression=Key(foreign_key).eq(foreign_key_value),
                Limit=limit,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )

        logger.info(
            f"Related data copied from table {source_table_name} to {target_table_name}"
        )

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8)
    )
    def _retry_put_item(self, batch, item):
        """
        Retries putting an item into a batch writer with exponential backoff.

        Args:
            batch: The batch writer object.
            item (dict): The item to put into the batch.
        """
        try:
            batch.put_item(Item=item)
            logger.debug(f"Copied item: {item}")
        except ClientError as e:
            logger.error(f"Error putting item: {item}, Error: {e}")
            raise

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8)
    )
    def copy_table_structure(self, source_table_name, target_table_name):
        """
        Copies the structure of a DynamoDB table from the source to the target.

        Args:
            source_table_name (str): The name of the source table.
            target_table_name (str): The name of the target table.
        """
        logger.info(
            f"Copying table structure from: {source_table_name} to {target_table_name}"
        )

        try:
            source_table = self.source_dynamodb.Table(source_table_name)
            response = source_table.meta.client.describe_table(
                TableName=source_table_name
            )
            table_structure = response["Table"]

            # Remove properties that are not allowed in create_table
            for attr in [
                "TableStatus",
                "CreationDateTime",
                "TableArn",
                "TableId",
                "ItemCount",
                "TableSizeBytes",
                "ProvisionedThroughput",
                "LatestStreamLabel",
                "LatestStreamArn",
                "RestoreSummary",
                "SSEDescription",
                "GlobalTableVersion",
            ]:
                table_structure.pop(attr, None)

            # Adjust provisioned throughput if needed (e.g., for local DynamoDB)
            if self.target_dynamodb == self.source_dynamodb:  # Same environment
                table_structure["ProvisionedThroughput"] = {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                }

            self.target_dynamodb.create_table(**table_structure)
            logger.info(
                f"Table structure copied from {source_table_name} to {target_table_name}"
            )

        except ClientError as e:
            logger.error(f"Error copying table structure from {source_table_name}: {e}")
            raise

    def table_exists(self, table_name, target=True):
        """
        Checks if a table exists in the target or source DynamoDB environment.

        Args:
            table_name (str): The name of the table to check.
            target (bool): Whether to check in the target (True) or source (False) environment.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        dynamodb_resource = self.target_dynamodb if target else self.source_dynamodb
        try:
            table = dynamodb_resource.Table(table_name)
            table.load()  # This will raise an exception if the table doesn't exist
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            else:
                logger.error(f"Error checking table existence for {table_name}: {e}")
                raise
