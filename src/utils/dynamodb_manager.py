import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from log_config import log_manager

logger = log_manager.get_logger(os.path.splitext(os.path.basename(__file__))[0])


class CompositeKey:
    separator = "#"
    secondary_separator = "/"

    @staticmethod
    def create(*entries: str, use_secondary: bool = False) -> str:
        separator = (
            CompositeKey.secondary_separator
            if use_secondary
            else CompositeKey.separator
        )
        return separator.join(entries)

    @staticmethod
    def get_keys(key: str, use_secondary: bool = False) -> Tuple[str, ...]:
        separator = (
            CompositeKey.secondary_separator
            if use_secondary
            else CompositeKey.separator
        )
        return tuple(key.split(separator))


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
            logger.error(f"No connection configuration found with name '{name}'.")
            return

        try:
            conn_config = self.connection_configs[name]
            conn_config.pop("name", None)
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

        response = source_table.scan(Limit=limit)
        total_copied = 0

        while response.get("Items", []) and total_copied < limit:
            items = response.get("Items", [])

            # Calculate how many items to copy in this batch
            items_to_copy = limit - total_copied

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
                Limit=limit - total_copied,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )

        logger.info(
            f"Data copied from table {source_name}.{source_table_name} to "
            f"{target_name}.{target_table_name}. Total items copied: {total_copied}"
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
        """
        Filters the table structure to remove unnecessary attributes.

        Args:
            table_structure (dict): The original table structure.

        Returns:
            dict: The filtered table structure.
        """
        # Retain only necessary attributes for table creation
        keys_to_retain = {
            "AttributeDefinitions",
            "TableName",
            "KeySchema",
            "ProvisionedThroughput",
            "GlobalSecondaryIndexes",
            "LocalSecondaryIndexes",
            "StreamSpecification",
        }
        return {
            key: table_structure[key]
            for key in keys_to_retain
            if key in table_structure
        }

    def _clean_indexes(self, filtered_structure: Dict[str, Any]):
        """
        Cleans and normalizes the index metadata within the given table structure.

        This method iterates through the GlobalSecondaryIndexes and LocalSecondaryIndexes
        in the provided table structure and removes unnecessary attributes that are not
        needed when creating a new table. It also ensures that the ProvisionedThroughput
        settings have sensible minimum values.

        Args:
            filtered_structure (dict): The table structure containing indexes to be cleaned.
        """
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

    def _clean_provisioned_throughput(self, structure: Dict[str, Any]) -> None:
        """
        Cleans the ProvisionedThroughput settings within the given table structure.

        This method removes the NumberOfDecreasesToday attribute from the ProvisionedThroughput
        settings and ensures that the ReadCapacityUnits and WriteCapacityUnits have sensible minimum values.

        Args:
            structure (dict): The table structure containing ProvisionedThroughput to be cleaned.
        """
        if "ProvisionedThroughput" in structure:
            structure["ProvisionedThroughput"].pop("NumberOfDecreasesToday", None)
            structure["ProvisionedThroughput"]["ReadCapacityUnits"] = max(
                1, structure["ProvisionedThroughput"].get("ReadCapacityUnits", 5)
            )
            structure["ProvisionedThroughput"]["WriteCapacityUnits"] = max(
                1, structure["ProvisionedThroughput"].get("WriteCapacityUnits", 5)
            )

    def copy_table_structure(
        self, source_name, source_table_name, target_name, target_table_name
    ):
        """
        Copies the structure of a source DynamoDB table to a target DynamoDB table.

        Args:
            source_name (str): The name of the source connection.
            source_table_name (str): The name of the source table.
            target_name (str): The name of the target connection.
            target_table_name (str): The name of the target table.
        """
        logger.info(
            f"Copying table structure from: {source_name}.{source_table_name} to {target_name}.{target_table_name}"
        )

        try:
            # Get connections once and reuse
            source_connection = self.get_connection(source_name)
            target_connection = self.get_connection(target_name)

            source_table = source_connection.Table(source_table_name)
            response = source_table.meta.client.describe_table(
                TableName=source_table_name
            )
            table_structure = response["Table"]

            # Filter the structure
            filtered_structure = self._filter_table_structure(table_structure)

            # Clean indexes
            self._clean_indexes(filtered_structure)

            # Clean ProvisionedThroughput for the table itself
            self._clean_provisioned_throughput(filtered_structure)

            # Remove StreamSpecification explicitly
            filtered_structure.pop("StreamSpecification", None)

            # Create the new table with the cleaned structure
            target_connection.create_table(**filtered_structure)
            logger.info(
                f"Table {target_table_name} created successfully in {target_name}."
            )
        except Exception as e:
            logger.error(f"Error copying table structure: {e}")
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

    def get_items_in_batches(
        self,
        connection_name: str,
        table_name: str,
        projection_expression: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Retrieve items from a DynamoDB table in batches.

        Args:
            connection_name (str): The name of the DynamoDB connection.
            table_name (str): The name of the table to retrieve items from.
            projection_expression (str): The projection expression to specify the attributes to retrieve.
            limit (int): The maximum number of items to retrieve in each batch.

        Returns:
            List[Dict[str, Any]]: List of items from the table.
        """
        table = self.get_connection(connection_name).Table(table_name)
        response = table.scan(
            ProjectionExpression=projection_expression,
            Limit=min(100, limit),
        )
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            response = table.scan(
                ProjectionExpression=projection_expression,
                ExclusiveStartKey=response["LastEvaluatedKey"],
                Limit=min(100, limit),
            )
            items.extend(response.get("Items", []))

        return items

    def query_partition_keys(self, connection_name, table_name, partition_keys):
        """
        Query DynamoDB using a list of partition keys.

        Args:
            connection_name (str): Connection name.
            table_name (str): DynamoDB table name.
            partition_keys (list): List of partition keys.

        Returns:
            list: Combined results from all partition key queries.
        """
        connection = self.get_connection(connection_name)
        table = connection.Table(table_name)
        results = []

        for pk in partition_keys:
            logger.info(f"Querying for partition key: {pk}")
            response = table.query(KeyConditionExpression=Key("pk").eq(pk))
            results.extend(response.get("Items", []))

            while "LastEvaluatedKey" in response:
                response = table.query(
                    KeyConditionExpression=Key("pk").eq(pk),
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                results.extend(response.get("Items", []))
        return results

    def query_partition_keys_parallel(
        self, connection_name, table_name, partition_keys
    ):
        """
        Parallelized version to query DynamoDB using multiple partition keys.

        Args:
            connection_name (str): Connection name.
            table_name (str): DynamoDB table name.
            partition_keys (list): List of partition keys.

        Returns:
            list: Combined results from all partition key queries.
        """
        connection = self.get_connection(connection_name)
        table = connection.Table(table_name)
        results = []

        def query_pk(pk):
            logger.info(f"Querying for partition key: {pk}")
            response = table.query(KeyConditionExpression=Key("pk").eq(pk))
            items = response.get("Items", [])

            while "LastEvaluatedKey" in response:
                response = table.query(
                    KeyConditionExpression=Key("pk").eq(pk),
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                items.extend(response.get("Items", []))
            return items

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(query_pk, pk) for pk in partition_keys]
            for future in futures:
                results.extend(future.result())

        return results

    def insert_record(
        self, connection_name: str, table_name: str, record: Dict[str, Any]
    ) -> None:
        """
        Inserts a single record into the specified DynamoDB table.

        Args:
            connection_name (str): Name of the DynamoDB connection.
            table_name (str): Name of the table to insert into.
            record (Dict[str, Any]): The record to insert.
        """
        try:
            table = self.get_connection(connection_name).Table(table_name)
            table.put_item(Item=record)
            logger.info(f"Inserted record into {table_name}: {record}")
        except ClientError as e:
            logger.error(f"Error inserting record into {table_name}: {e}")
            raise

    def insert_records_in_bulk(
        self, connection_name: str, table_name: str, records: List[Dict[str, Any]]
    ) -> None:
        """
        Inserts multiple records into a DynamoDB table in bulk, with progress updates.

        Args:
            connection_name (str): Name of the DynamoDB connection.
            table_name (str): Name of the table to insert into.
            records (List[Dict[str, Any]]): List of records to insert.
        """
        try:
            table = self.get_connection(connection_name).Table(table_name)
            total_records = len(records)
            with table.batch_writer() as batch:
                for i, record in enumerate(records, start=1):
                    batch.put_item(Item=record)
                    if i % 10 == 0 or i == total_records:
                        progress = (i / total_records) * 100
                        sys.stdout.write(
                            f"\rProgress: {i}/{total_records} records inserted ({progress:.2f}%)"
                        )
                        sys.stdout.flush()
            logger.info(
                f"Successfully inserted {total_records} records into {table_name}."
            )
        except ClientError as e:
            logger.error(f"Error during bulk insert into {table_name}: {e}")
            raise

    def insert_records_with_retries(
        self,
        connection_name: str,
        table_name: str,
        records: List[Dict[str, Any]],
        retries: int = 3,
    ) -> None:
        """
        Inserts multiple records into a DynamoDB table with retries for unprocessed items, with progress updates.

        Args:
            connection_name (str): Name of the DynamoDB connection.
            table_name (str): Name of the table to insert into.
            records (List[Dict[str, Any]]): List of records to insert.
            retries (int): Number of retries for unprocessed items.
        """
        chunk_size = 25
        total_records = len(records)
        for start_idx in range(0, total_records, chunk_size):
            chunk = records[start_idx : start_idx + chunk_size]
            attempt = 0

            while attempt <= retries:
                try:
                    self.insert_records_in_bulk(connection_name, table_name, chunk)
                    progress = ((start_idx + len(chunk)) / total_records) * 100
                    sys.stdout.write(
                        f"\rProgress: {start_idx + len(chunk)}/{total_records} records processed ({progress:.2f}%)"
                    )
                    sys.stdout.flush()
                    break  # Exit retry loop if successful
                except Exception as e:
                    attempt += 1
                    if attempt > retries:
                        logger.error(
                            f"Failed to insert records after {retries} retries: {chunk}"
                        )
                        raise
                    logger.warning(
                        f"Retrying batch insert ({attempt}/{retries}) due to error: {e}"
                    )
                    time.sleep(2**attempt)
        sys.stdout.write("\n")
