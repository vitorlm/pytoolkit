import logging
import sys
import time

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class DynamoDBManager:
    def __init__(self):
        """
        Initializes the DynamoDBManager without creating connections initially.
        Connections will be established lazily when required.
        """
        self.connections = {}
        self.connection_configs = {}

    def add_connection_config(self, conn_config):
        """
        Adds a new DynamoDB connection configuration to the manager.

        Args:
            conn_config (dict): Configuration for the new DynamoDB connection.
                                Must contain a "name" key.
        """
        try:
            name = conn_config["name"]
            if name in self.connection_configs:
                raise ValueError(
                    f"Connection configuration with name '{name}' already exists."
                )
            self.connection_configs[name] = conn_config
            logger.info(f"Added configuration for connection '{name}'.")
        except Exception as e:
            logger.error(f"Error adding connection configuration '{name}': {e}")

    def _initialize_connection(self, name):
        """
        Initializes a DynamoDB connection based on the stored configuration.

        Args:
            name (str): The name of the connection to initialize.
        """
        if name not in self.connection_configs:
            raise ValueError(f"No connection configuration found with name '{name}'.")

        try:
            conn_config = self.connection_configs[name]
            if "endpoint_url" in conn_config:
                conn = boto3.resource("dynamodb", **conn_config)
            else:
                conn = boto3.resource("dynamodb", **conn_config)
            self.connections[name] = conn
            logger.info(f"Connection '{name}' initialized successfully.")
        except Exception as e:
            logger.error(f"Error initializing connection '{name}': {e}")
            raise

    def get_connection(self, name):
        """
        Retrieves a DynamoDB connection by its name, initializing it if necessary.

        Args:
            name (str): The name of the connection.

        Returns:
            boto3.resources.dynamodb.ServiceResource: The DynamoDB connection.
        """
        if name not in self.connections:
            self._initialize_connection(name)
        return self.connections[name]

    def copy_table_data(
        self, source_name, source_table_name, target_name, target_table_name, limit=100
    ):
        """
        Copies data from a source table to a target table in DynamoDB with pagination.

        Args:
            source_name (str): The name of the source connection.
            source_table_name (str): The name of the source table.
            target_name (str): The name of the target connection.
            target_table_name (str): The name of the target table.
            limit (int): Maximum number of items to copy.
        """
        logger.info(
            f"Copying data from table: {source_table_name} to {target_table_name}, limit: {limit} items"
        )

        source_table = self.get_connection(source_name).Table(source_table_name)
        target_table = self.get_connection(target_name).Table(target_table_name)

        response = source_table.scan(
            Limit=min(100, limit)
        )  # Initial scan with up to 100 items
        total_copied = 0

        while response.get("Items", []) and total_copied < limit:
            items = response.get("Items", [])

            # Calculate how many items to copy in this batch
            items_to_copy = min(len(items), limit - total_copied)

            with target_table.batch_writer() as batch:
                for item in items[:items_to_copy]:
                    self._retry_put_item(batch, item)
                    total_copied += 1

                    # Print progress using logger in the same line
                    log_message = f"Progress: {total_copied}/{limit} items copied"
                    sys.stdout.write(
                        f"\r{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}"
                    )
                    sys.stdout.flush()

                    # Stop if we've reached the limit
                    if total_copied >= limit:
                        sys.stdout.write("\n")
                        logger.info("Reached the copy limit.")
                        break

            if total_copied >= limit or "LastEvaluatedKey" not in response:
                break

            # Continue scanning the next batch of items
            response = source_table.scan(
                Limit=min(100, limit - total_copied),
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )

        logger.info(
            f"Data copied from table {source_table_name} to {target_table_name}. Total items copied: {total_copied}"
        )

    def copy_related_data(
        self,
        source_name,
        source_table_name,
        target_name,
        target_table_name,
        foreign_key,
        foreign_key_value,
        index_name,
        limit=100,
    ):
        """
        Copies related data from a source table to a target table based on a foreign key.

        Args:
            source_name (str): The name of the source connection.
            source_table_name (str): The name of the source table.
            target_name (str): The name of the target connection.
            target_table_name (str): The name of the target table.
            foreign_key (str): The foreign key to filter items.
            foreign_key_value (str): The value of the foreign key to filter items.
            index_name (str): The index name to use for the query.
            limit (int): Maximum number of items to query at a time.
        """
        logger.info(
            f"Copying related data from {source_table_name} to {target_table_name} based on foreign key: {foreign_key}"
        )

        source_table = self.get_connection(source_name).Table(source_table_name)
        target_table = self.get_connection(target_name).Table(target_table_name)

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

    def table_exists(self, table_name, connection_name):
        """
        Checks if a table exists in the specified DynamoDB connection.

        Args:
            table_name (str): The name of the table to check.
            connection_name (str): The name of the connection.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            table = self.get_connection(connection_name).Table(table_name)
            table.load()  # This will raise an exception if the table doesn't exist
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            else:
                logger.error(f"Error checking table existence for {table_name}: {e}")
                raise

    def _filter_table_structure(self, table_structure):
        # Filtra os parâmetros válidos da estrutura da tabela
        valid_params = {
            "AttributeDefinitions",
            "TableName",
            "KeySchema",
            "LocalSecondaryIndexes",
            "GlobalSecondaryIndexes",
            "BillingMode",
            "ProvisionedThroughput",
            "StreamSpecification",
            "SSESpecification",
            "Tags",
            "TableClass",
            "DeletionProtectionEnabled",
        }
        return {k: v for k, v in table_structure.items() if k in valid_params}

    def _clean_indexes(self, filtered_structure):
        for index_type in ["GlobalSecondaryIndexes", "LocalSecondaryIndexes"]:
            if index_type in filtered_structure:
                for index in filtered_structure[index_type]:
                    for key in [
                        "IndexStatus",
                        "IndexSizeBytes",
                        "ItemCount",
                        "IndexArn",
                        "WarmThroughput",
                    ]:
                        index.pop(key, None)

                    if "ProvisionedThroughput" in index:
                        index["ProvisionedThroughput"].pop(
                            "NumberOfDecreasesToday", None
                        )
                        index["ProvisionedThroughput"]["ReadCapacityUnits"] = max(
                            1,
                            index["ProvisionedThroughput"].get("ReadCapacityUnits", 5),
                        )
                        index["ProvisionedThroughput"]["WriteCapacityUnits"] = max(
                            1,
                            index["ProvisionedThroughput"].get("WriteCapacityUnits", 5),
                        )

    def copy_table_structure(
        self, source_name, source_table_name, target_name, target_table_name
    ):
        """
        Copies the structure of a DynamoDB table from the source to the target.

        Args:
            source_name (str): The name of the source connection.
            source_table_name (str): The name of the source table.
            target_name (str): The name of the target connection.
            target_table_name (str): The name of the target table.
        """
        logger.info(
            f"Copying table structure from: {source_table_name} to {target_table_name}"
        )

        try:
            # Obtenha conexões de uma vez e reutilize
            source_connection = self.get_connection(source_name)
            target_connection = self.get_connection(target_name)

            source_table = source_connection.Table(source_table_name)
            response = source_table.meta.client.describe_table(
                TableName=source_table_name
            )
            table_structure = response["Table"]

            # Filtrar a estrutura
            filtered_structure = self._filter_table_structure(table_structure)

            # Limpar índices globais
            self._clean_indexes(filtered_structure)

            # Remover explicitamente StreamSpecification
            filtered_structure.pop("StreamSpecification", None)

            # Ajustar throughput para ambientes locais
            if source_connection == target_connection:
                filtered_structure["ProvisionedThroughput"] = {
                    "ReadCapacityUnits": 5,
                    "WriteCapacityUnits": 5,
                }

            # Criar tabela no destino
            target_connection.create_table(**filtered_structure)
            logger.info(
                f"Table structure copied from {source_table_name} to {target_table_name}"
            )

        except Exception as e:
            logger.exception(
                f"Unexpected error copying table structure from {source_table_name} to {target_name}: {e}"
            )
            raise

    def get_data_with_filter(
        self, connection_name, table_name, filter_expression=None, limit=None
    ):
        """
        Retrieves data from a DynamoDB table with optional filter and limit.

        Args:
            connection_name (str): The name of the DynamoDB connection.
            table_name (str): The name of the table to extract data from.
            filter_expression (boto3.dynamodb.conditions.Condition, optional):
                A filter expression to apply to the query. Defaults to None.
            limit (int, optional): The maximum number of items to retrieve.
                Defaults to None.

        Returns:
            list: A list of DynamoDB items.
        """
        try:
            table = self.get_connection(connection_name).Table(table_name)

            scan_kwargs = {}
            if filter_expression:
                scan_kwargs["FilterExpression"] = filter_expression
            if limit:
                scan_kwargs["Limit"] = limit

            response = table.scan(**scan_kwargs)
            items = response.get("Items", [])

            # Continue scanning until all items are retrieved (respecting limit)
            while "LastEvaluatedKey" in response and (not limit or len(items) < limit):
                scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
                response = table.scan(**scan_kwargs)
                items.extend(response.get("Items", []))

            # Apply limit after all items are retrieved
            if limit:
                items = items[:limit]

            return items

        except Exception as e:
            logger.error(f"Error retrieving data from table: {e}")
            raise
