import decimal
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import concurrent.futures

from utils.cache_manager.cache_manager import CacheManager
from utils.file_manager import FileManager
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager

import pyarrow as pa
import pyarrow.parquet as pq


class CompositeKey:
    separator = "#"
    secondary_separator = "/"

    @staticmethod
    def create(*entries: str, use_secondary: bool = False) -> str:
        separator = CompositeKey.secondary_separator if use_secondary else CompositeKey.separator
        return separator.join(entries)

    @staticmethod
    def get_keys(key: str, use_secondary: bool = False) -> Tuple[str, ...]:
        separator = CompositeKey.secondary_separator if use_secondary else CompositeKey.separator
        return tuple(key.split(separator))


class DynamoDBManager:

    _logger = LogManager.get_instance().get_logger("DynamoDBManager")

    def __init__(self, cache_expiration: Optional[int] = 3600):
        """
        Initializes the DynamoDBManager without creating connections initially.
        Connections will be established lazily when required.
        """
        self.connections = {}
        self.connection_configs = {}
        self.cache_manager = CacheManager.get_instance()
        self.cache_expiration = cache_expiration

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
                raise ValueError(f"Connection configuration with name '{name}' already exists.")
            self.connection_configs[name] = conn_config
            self._logger.info(f"Added configuration for connection '{name}'.")
        except Exception as e:
            self._logger.error(f"Error adding connection configuration '{name}': {e}")

    def _initialize_connection(self, name):
        """
        Initializes a DynamoDB connection based on the stored configuration.

        Args:
            name (str): The name of the connection to initialize.
        """
        if name not in self.connection_configs:
            self._logger.error(f"No connection configuration found with name '{name}'.")
            return

        try:
            conn_config = self.connection_configs[name]
            conn_config.pop("name", None)
            conn = boto3.resource("dynamodb", **conn_config)
            self.connections[name] = conn
            self._logger.info(f"Connection '{name}' initialized successfully.")
        except Exception as e:
            self._logger.error(f"Error initializing connection '{name}': {e}")
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
        self,
        source_name,
        source_table_name,
        target_name,
        target_table_name,
        limit=100,
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
        self._logger.info(
            f"Copying data from table: {source_table_name} to {target_table_name}, "
            f"limit: {limit} items"
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

                    # Print progress using self._logger in the same line
                    log_message = f"Progress: {total_copied}/{limit} items copied"
                    sys.stdout.write(
                        f"\r{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - {log_message}"
                    )
                    sys.stdout.flush()

                    # Stop if we've reached the limit
                    if total_copied >= limit:
                        sys.stdout.write("\n")
                        self._logger.info("Reached the copy limit.")
                        break

            if total_copied >= limit or "LastEvaluatedKey" not in response:
                break

            # Continue scanning the next batch of items
            response = source_table.scan(
                Limit=limit - total_copied,
                ExclusiveStartKey=response["LastEvaluatedKey"],
            )

        self._logger.info(
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
        self._logger.info(
            f"Copying related data from {source_table_name} to {target_table_name} based "
            f"on foreign key: {foreign_key}"
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

        self._logger.info(
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
            self._logger.debug(f"Copied item: {item}")
        except ClientError as e:
            self._logger.error(f"Error putting item: {item}, Error: {e}")
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
                self._logger.error(f"Error checking table existence for {table_name}: {e}")
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
        return {key: table_structure[key] for key in keys_to_retain if key in table_structure}

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
                        index["ProvisionedThroughput"].pop("NumberOfDecreasesToday", None)
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

        This method removes the NumberOfDecreasesToday attribute from the
        ProvisionedThroughput settings and ensures that the ReadCapacityUnits
        and WriteCapacityUnits have sensible minimum values

        Args:
            structure (dict): The table structure containing ProvisionedThroughput to be cleaned.
        """
        if "ProvisionedThroughput" in structure:
            structure["ProvisionedThroughput"].pop("NumberOfDecreasesToday", None)
            structure["ProvisionedThroughput"]["ReadCapacityUnits"] = max(
                1,
                structure["ProvisionedThroughput"].get("ReadCapacityUnits", 5),
            )
            structure["ProvisionedThroughput"]["WriteCapacityUnits"] = max(
                1,
                structure["ProvisionedThroughput"].get("WriteCapacityUnits", 5),
            )

    def copy_table_structure(self, source_name, source_table_name, target_name, target_table_name):
        """
        Copies the structure of a source DynamoDB table to a target DynamoDB table.

        Args:
            source_name (str): The name of the source connection.
            source_table_name (str): The name of the source table.
            target_name (str): The name of the target connection.
            target_table_name (str): The name of the target table.
        """
        self._logger.info(
            f"Copying table structure from: {source_name}.{source_table_name} "
            f"to {target_name}.{target_table_name}"
        )

        try:
            # Get connections once and reuse
            source_connection = self.get_connection(source_name)
            target_connection = self.get_connection(target_name)

            source_table = source_connection.Table(source_table_name)
            response = source_table.meta.client.describe_table(TableName=source_table_name)
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
            self._logger.info(f"Table {target_table_name} created successfully in {target_name}.")
        except Exception as e:
            self._logger.error(f"Error copying table structure: {e}")
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
            projection_expression (str): The projection expression to
                                         specify the attributes to retrieve.
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
            self._logger.info(f"Querying for partition key: {pk}")
            response = table.query(KeyConditionExpression=Key("pk").eq(pk))
            results.extend(response.get("Items", []))

            while "LastEvaluatedKey" in response:
                response = table.query(
                    KeyConditionExpression=Key("pk").eq(pk),
                    ExclusiveStartKey=response["LastEvaluatedKey"],
                )
                results.extend(response.get("Items", []))
        return results

    def query_partition_keys_parallel(self, connection_name, table_name, partition_keys):
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
            self._logger.info(f"Querying for partition key: {pk}")
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

    def insert_record(self, connection_name: str, table_name: str, record: Dict[str, Any]) -> None:
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
            self._logger.info(f"Inserted record into {table_name}: {record}")
        except ClientError as e:
            self._logger.error(f"Error inserting record into {table_name}: {e}")
            raise

    def insert_records_in_bulk(
        self,
        connection_name: str,
        table_name: str,
        records: List[Dict[str, Any]],
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
            self._logger.info(f"Successfully inserted {total_records} records into {table_name}.")
        except ClientError as e:
            self._logger.error(f"Error during bulk insert into {table_name}: {e}")
            raise

    def insert_records_with_retries(
        self,
        connection_name: str,
        table_name: str,
        records: List[Dict[str, Any]],
        retries: int = 3,
    ) -> None:
        """
        Inserts multiple records into a DynamoDB table with retries for
        unprocessed items, with progress updates.

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
                        f"\rProgress: {start_idx + len(chunk)}/{total_records} "
                        f"records processed ({progress:.2f}%)"
                    )
                    sys.stdout.flush()
                    break  # Exit retry loop if successful
                except Exception as e:
                    attempt += 1
                    if attempt > retries:
                        self._logger.error(
                            f"Failed to insert records after {retries} retries: {chunk}"
                        )
                        raise
                    self._logger.warning(
                        f"Retrying batch insert ({attempt}/{retries}) due to error: {e}"
                    )
                    time.sleep(2**attempt)
        sys.stdout.write("\n")

    def get_data_with_filter(
        self,
        connection_name: str,
        table_name: str,
        filter_expression: Optional[Any] = None,
        limit: Optional[int] = None,
        max_workers: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Retrieves data from a DynamoDB table with parallel scans and threads for performance.

        Args:
            connection_name (str): The name of the DynamoDB connection.
            table_name (str): The name of the table to extract data from.
            filter_expression (Optional[Condition]): Filter expression for the scan.
            limit (Optional[int]): Maximum number of items to retrieve.
            max_workers (int): Number of parallel threads for segment scanning.

        Returns:
            List[Dict[str, Any]]: List of DynamoDB items.
        """
        try:

            # cache_key = self.cache_manager.generate_cache_key(
            #     "dynamodb",
            #     connection_name=connection_name,
            #     table_name=table_name,
            #     filter_expression=filter_expression,
            #     limit=limit,
            # )
            # cached_data = self._load_from_cache(cache_key)
            # if cached_data:
            #     self._logger.info("Loaded data from cache.")
            #     return cached_data

            # Log inicial com parâmetros
            self._logger.info(
                f"Starting parallel scan on {table_name} "
                f"[Filter: {filter_expression}, Limit: {limit}, Workers: {max_workers}]"
            )

            start_time = time.time()
            table = self.get_connection(connection_name).Table(table_name)
            total_segments = max_workers
            items = []
            remaining_limit = limit

            # Contador atômico para progresso
            progress_counter = {"value": 0, "lock": threading.Lock()}

            def _update_progress(items_count: int):
                """Atualiza o log de progresso de forma thread-safe"""
                with progress_counter["lock"]:
                    progress_counter["value"] += items_count
                    current_total = progress_counter["value"]

                    message = f"Scanned {current_total}"
                    if limit:
                        message += f"/{limit} ({min(100, int(current_total / limit * 100))}%)"
                    sys.stdout.write(f"\r{message.ljust(40)}")
                    sys.stdout.flush()

            def scan_segment(segment: int):
                """Função executada por cada thread"""
                try:
                    self._logger.debug(f"Segment {segment} started")
                    local_items = []
                    scan_kwargs = {"Segment": segment, "TotalSegments": total_segments}

                    if filter_expression:
                        scan_kwargs["FilterExpression"] = filter_expression
                    if remaining_limit:
                        scan_kwargs["Limit"] = min((remaining_limit // total_segments) or 1, 100)

                    # Primeira página
                    response = table.scan(**scan_kwargs)
                    local_items.extend(response.get("Items", []))
                    _update_progress(len(response.get("Items", [])))

                    # Páginas subsequentes
                    while "LastEvaluatedKey" in response:
                        if remaining_limit and len(local_items) >= (
                            remaining_limit // total_segments
                        ):
                            break

                        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
                        response = table.scan(**scan_kwargs)
                        local_items.extend(response.get("Items", []))
                        _update_progress(len(response.get("Items", [])))

                        # Log detalhado a cada 100 itens no segmento
                        if len(local_items) % 100 == 0:
                            self._logger.debug(
                                f"Segment {segment} progress: {len(local_items)} items"
                            )

                    self._logger.debug(f"Segment {segment} completed: {len(local_items)} items")
                    return local_items

                except Exception as e:
                    self._logger.error(f"Segment {segment} failed: {e}")
                    raise

            # Execução paralela
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(scan_segment, s) for s in range(total_segments)]

                for future in futures:
                    segment_items = future.result()
                    items.extend(segment_items)

                    if remaining_limit:
                        remaining_limit -= len(segment_items)
                        if remaining_limit <= 0:
                            executor.shutdown(wait=False)
                            for f in futures:
                                f.cancel()
                            break

            # Log final
            sys.stdout.write("\n")
            duration = time.time() - start_time
            self._logger.info(
                f"Scan completed. Total items: {len(items)} "
                f"(Duration: {duration:.2f}s, "
                f"Avg speed: {len(items) / max(duration, 0.1):.1f} items/s)"
            )

            if limit:
                items = items[:limit]

            # self._save_to_cache(cache_key, items)

            return items

        except Exception as e:
            self._logger.error(f"Error retrieving data from table: {e}")
            raise

    def _load_from_cache(self, cache_key: str) -> Optional[Dict]:
        """
        Loads data from the cache if available and valid.

        Args:
            cache_key (str): The cache key to retrieve data for.

        Returns:
            Optional[Dict]: Cached data if available, None otherwise.
        """
        try:
            return self.cache_manager.load(cache_key, expiration_minutes=self.cache_expiration)
        except Exception as e:
            self._logger.warning(f"Cache miss or load failure for key '{cache_key}': {e}")
            return None

    def _save_to_cache(self, cache_key: str, data: Dict):
        """
        Saves data to the cache.

        Args:
            cache_key (str): The cache key to store data for.
            data (Dict): The data to be cached.
        """
        try:
            self.cache_manager.save(cache_key, data)
            self._logger.info(f"Data cached under key: {cache_key}")
        except Exception as e:
            self._logger.error(f"Failed to cache data for key '{cache_key}': {e}")

    def _get_next_part_index(self, segment: int) -> int:
        """Retorna o próximo índice de parte para o segmento, baseado nos arquivos existentes no
        diretório atual."""
        prefix = f"temp_segment_{segment}_part_"
        existing_parts = []
        for fname in os.listdir("."):
            if fname.startswith(prefix) and fname.endswith(".parquet"):
                try:
                    part = int(fname[len(prefix) : -len(".parquet")])
                    existing_parts.append(part)
                except Exception:
                    continue
        if existing_parts:
            return max(existing_parts) + 1
        else:
            return 0

    def scan_dynamodb_to_parquet_with_checkpoint(
        self,
        connection_name: str,
        table_name: str,
        filter_expression: Optional[Any] = None,
        limit: Optional[int] = None,
        max_workers: int = 15,
        output_parquet_path: str = "data/output.parquet",
        checkpoint_path: str = "data/dynamodb_scan_checkpoint.json",
        scan_batch_limit: int = 1000,
    ) -> None:
        """
        Scaneia incrementalmente uma tabela do DynamoDB utilizando segmentos paralelos, grava cada
        lote em
        um arquivo Parquet parcial (armazenando cada registro como JSON na coluna "raw_record") e
        armazena o checkpoint (LastEvaluatedKey) em um arquivo JSON.
        Em caso de falha, o scan pode ser retomado a partir do último checkpoint, utilizando os
        arquivos parciais já gravados.

        Após a conclusão, os arquivos parciais são mesclados em um único arquivo Parquet.

        Args:
            connection_name (str): Nome da conexão DynamoDB.
            table_name (str): Nome da tabela DynamoDB.
            filter_expression (Optional[Any]): Expressão de filtro para o scan.
            limit (Optional[int]): Limite total de itens a serem lidos (dividido aproximadamente
            por segmento).
            max_workers (int): Número de segmentos/workers paralelos.
            output_parquet_path (str): Caminho para o arquivo Parquet final.
            checkpoint_path (str): Caminho para o arquivo de checkpoint JSON.
            scan_batch_limit (int): Limite passado para cada chamada de scan (tamanho do lote).
        """
        self._logger.info(
            f"Starting incremental parallel scan on {table_name} "
            f"[Filter: {filter_expression}, Limit: {limit}, Workers: {max_workers}]"
        )

        # Carrega o checkpoint se existir; caso contrário, inicia um novo.
        if FileManager.file_exists(checkpoint_path):
            checkpoint = JSONManager.read_json(checkpoint_path)
            self._logger.info(f"Resuming scan using checkpoint file: {checkpoint_path}")
        else:
            checkpoint = {str(seg): None for seg in range(max_workers)}
            self._logger.info("No checkpoint file found; starting fresh scan.")

        checkpoint_lock = threading.Lock()

        def update_checkpoint(segment: int, last_key: Any) -> None:
            with checkpoint_lock:
                checkpoint[str(segment)] = last_key
                JSONManager.write_json(checkpoint, checkpoint_path)

        total_segments = max_workers
        table = self.get_connection(connection_name).Table(table_name)

        # Dicionário para armazena\r o progresso de cada segmento.
        progress_lock = threading.Lock()
        segment_progress = {str(seg): 0 for seg in range(max_workers)}
        progress_stop_event = threading.Event()

        def log_aggregated_progress():
            while not progress_stop_event.is_set():
                time.sleep(1)
                with progress_lock:
                    total = sum(segment_progress[str(seg)] for seg in range(max_workers))
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                    progress_line = f"[{timestamp}][INFO][DynamoDBManager]: Progress - {total:>8}."

                sys.stdout.write("\r" + progress_line)
                sys.stdout.flush()
            sys.stdout.write("\n")

        progress_thread = threading.Thread(target=log_aggregated_progress)
        progress_thread.start()

        def scan_segment_to_parquet(segment: int) -> None:
            start_key = checkpoint.get(str(segment))
            part_index = self._get_next_part_index(segment)
            temp_file = f"data/temp_segment_{segment}_part_{part_index}.parquet"
            writer = None
            segment_item_count = 0

            scan_kwargs = {
                "Segment": segment,
                "TotalSegments": total_segments,
                "Limit": scan_batch_limit,
            }
            if filter_expression:
                scan_kwargs["FilterExpression"] = filter_expression
            if start_key not in (None, "DONE"):
                scan_kwargs["ExclusiveStartKey"] = start_key

            backoff = 1
            max_backoff = 16

            while True:
                try:
                    response = table.scan(**scan_kwargs)
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in [
                        "ProvisionedThroughputExceededException",
                        "ThrottlingException",
                    ]:
                        self._logger.warning(
                            f"Segment {segment} throttled (error: {error_code}). "
                            f"Sleeping {backoff}s before retrying."
                        )
                        time.sleep(backoff)
                        backoff = min(backoff * 2, max_backoff)
                        continue
                    else:
                        self._logger.error(f"Segment {segment} failed with error: {e}")
                        raise
                backoff = 1
                items = response.get("Items", [])
                if items:
                    # Converte cada registro para JSON; objetos Decimal são convertidos para float.
                    normalized_items = [
                        {
                            "raw_record": json.dumps(
                                item,
                                default=lambda o: (
                                    float(o) if isinstance(o, decimal.Decimal) else str(o)
                                ),
                            )
                        }
                        for item in items
                    ]
                    try:
                        table_batch = pa.Table.from_pylist(normalized_items)
                    except Exception as conv_err:
                        self._logger.error(
                            f"Error converting items to PyArrow Table in segment {segment}: "
                            f"{conv_err}"
                        )
                        raise
                    if writer is None:
                        writer = pq.ParquetWriter(temp_file, table_batch.schema)
                    writer.write_table(table_batch)
                    segment_item_count += len(items)
                    with progress_lock:
                        segment_progress[str(segment)] = segment_item_count

                if "LastEvaluatedKey" in response:
                    last_key = response["LastEvaluatedKey"]
                    update_checkpoint(segment, last_key)
                    scan_kwargs["ExclusiveStartKey"] = last_key
                else:
                    update_checkpoint(segment, "DONE")
                    break

                if limit and segment_item_count >= (limit // total_segments):
                    update_checkpoint(segment, "DONE")
                    break

            if writer is not None:
                writer.close()
            self._logger.info(f"Segment {segment} finished with {segment_item_count} items.")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(scan_segment_to_parquet, seg) for seg in range(total_segments)
            ]
            concurrent.futures.wait(futures)

        progress_stop_event.set()
        progress_thread.join()

        self._logger.info(
            "All segments finished scanning. Proceeding to merge partial Parquet files."
        )

        temp_files = []
        for seg in range(total_segments):
            prefix = f"temp_segment_{seg}_part_"
            for fname in os.listdir("."):
                if fname.startswith(prefix) and fname.endswith(".parquet"):
                    temp_files.append(fname)
        if not temp_files:
            self._logger.error("No partial Parquet files were found.")
            return

        tables = []
        for temp in temp_files:
            try:
                t = pq.read_table(temp)
                tables.append(t)
            except Exception as e:
                self._logger.error(f"Error reading partial file {temp}: {e}")
                raise

        final_table = pa.concat_tables(tables)
        pq.write_table(final_table, output_parquet_path)
        self._logger.info(f"Final Parquet file written to {output_parquet_path}")
