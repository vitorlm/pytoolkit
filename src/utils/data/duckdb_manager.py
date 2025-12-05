import concurrent.futures
import datetime
import decimal
import json
import sys
import threading
from typing import Any

import duckdb

from utils.data.duckdb_data_validator import DuckDBDataValidator
from utils.logging.logging_manager import LogManager


class DuckDBManager:
    """A manager for handling DuckDB operations.
    This class allows saving data to DuckDB from Pandas DataFrames
    and efficiently retrieving it for use in tools like DBeaver.
    """

    def __init__(self):
        """Initializes DuckDBManager with lazy connection initialization.
        Connections are created when first accessed.
        """
        self._logger = LogManager.get_instance().get_logger("DuckDBManager")
        self.connections = {}
        self.connection_configs = {}
        self.data_validator = DuckDBDataValidator()

    def add_connection_config(self, conn_config: dict[str, Any]) -> None:
        """Adds a new DuckDB connection configuration.

        Args:
            conn_config (dict): Must contain 'name' and 'path' keys.
                                Optional 'read_only' flag (default=False).

        Raises:
            ValueError: If connection name already exists.
        """
        try:
            name = conn_config["name"]
            if name in self.connection_configs:
                raise ValueError(f"Connection '{name}' already exists")
            self.connection_configs[name] = conn_config
            self._logger.info(f"Added connection config: {name}")
        except KeyError:
            self._logger.error("Connection config requires 'name' and 'path' keys")
            raise

    def _initialize_connection(self, name: str) -> None:
        """Initializes a DuckDB connection using stored configuration.

        Args:
            name (str): Connection name configured via add_connection_config

        Raises:
            ValueError: If configuration not found
            RuntimeError: If connection fails
        """
        if name not in self.connection_configs:
            self._logger.error(f"Connection config '{name}' not found")
            raise ValueError(f"Connection '{name}' not configured")

        config = self.connection_configs[name]
        try:
            self.connections[name] = duckdb.connect(database=config["path"], read_only=config.get("read_only", False))
            self._logger.info(f"Initialized connection: {name}")
        except Exception as e:
            self._logger.error(f"Connection failed for '{name}': {e}")
            raise RuntimeError(f"DuckDB connection error: {e}") from None

    def get_connection(self, name: str) -> duckdb.DuckDBPyConnection:
        """Retrieves a DuckDB connection, initializing if necessary.

        Args:
            name (str): Connection name

        Returns:
            duckdb.DuckDBPyConnection: Active database connection
        """
        if name not in self.connections:
            self._initialize_connection(name)
        return self.connections[name]

    def create_table(
        self,
        connection_name: str,
        table_name: str,
        schema: dict[str, str] | None = None,
        sample_data: list[dict] | None = None,
    ) -> dict[str, str]:
        """Creates a table with optional schema definition or auto-detection.

        Args:
            connection_name (str): Target connection
            table_name (str): Name of table to create
            schema (dict): Column names -> data types mapping
            sample_data (list): Data for schema auto-detection

        Raises:
            ValueError: If neither schema nor sample_data provided
        """
        conn = self.get_connection(connection_name)

        if not schema and not sample_data:
            raise ValueError("Either schema or sample_data must be provided")

        if schema:
            columns = ", ".join([f"{k} {v}" for k, v in schema.items()])
        else:
            schema = self.data_validator.infer_schema(sample_data)
            columns = ", ".join([f'"{k}" {v}' for k, v in schema.items()])

        try:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})")
            self._logger.info(f"Created table {table_name} with schema: {schema}")
            return schema
        except duckdb.ParserException as e:
            self._logger.error(f"Failed to create table {table_name}: {e}")
            raise
        except duckdb.Error as e:
            self._logger.error(f"Failed to create table {table_name}: {e}")
            raise

    def insert_records(
        self,
        connection_name: str,
        table_name: str,
        schema: dict[str, str],
        records: list[dict[str, Any]],
        batch_size: int = 2500,
    ) -> None:
        """Insere registros na tabela especificada utilizando múltiplas threads para realizar os
        inserts em paralelo.

        Args:
            connection_name (str): Nome da conexão com o banco de dados.
            table_name (str): Nome da tabela onde os registros serão inseridos.
            schema (Dict[str, str]): Esquema da tabela, mapeando os nomes das colunas para seus
            tipos de dados.
            records (List[Dict[str, Any]]): Lista de registros a serem inseridos, onde cada
            registro é um dicionário.
            batch_size (int, optional): Número de registros a serem inseridos em cada batch.
            Padrão é 1000.

        Returns:
            None
        Raises:
            Exception: Caso ocorra algum erro durante o processo de inserção.
        Logs:
            Info: Loga o total de registros a serem inseridos e mensagem de conclusão.
            Warning: Loga quaisquer registros inválidos que forem ignorados.
            Progress: Exibe o progresso da inserção no console.
        """

        def convert_json_serializable(obj):
            if isinstance(obj, decimal.Decimal):
                return float(obj)
            if isinstance(obj, datetime.datetime):
                return obj.strftime("%Y-%m-%d %H:%M:%S")
            if isinstance(obj, list):
                return [convert_json_serializable(item) for item in obj]
            if isinstance(obj, dict):
                return {key: convert_json_serializable(value) for key, value in obj.items()}
            return obj

        # Obter conexão para recuperar o esquema, se necessário
        conn = self.get_connection(connection_name)
        total = len(records)
        self._logger.info(f"Inserting {total} records into {table_name}")

        if not schema:
            schema = conn.execute(f"DESCRIBE {table_name}").fetchall()

        # Validação e pré-processamento dos registros
        valid_records = []
        for record in records:
            if self.data_validator.validate_record(record, schema):
                for key, value in record.items():
                    if isinstance(value, dict):
                        record[key] = json.dumps(convert_json_serializable(value))
                    elif isinstance(value, str) and "T" in value and "Z" in value and "#" not in value:
                        try:
                            record[key] = datetime.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                        except ValueError:
                            record[key] = value
                valid_records.append(record)
            else:
                self._logger.warning(f"Skipping invalid record: {record}")

        total_valid = len(valid_records)
        if total_valid == 0:
            self._logger.info("No valid records to insert.")
            return

        # Particiona os registros válidos em batches
        batches = [valid_records[i : i + batch_size] for i in range(0, total_valid, batch_size)]

        # Variável compartilhada e lock para controle do progresso
        inserted_count = 0
        progress_lock = threading.Lock()

        def insert_batch(batch):
            nonlocal inserted_count
            # Cada thread obtém sua própria conexão
            thread_conn = self.get_connection(connection_name)
            for rec in batch:
                try:
                    placeholders = ", ".join(["?" for _ in rec])
                    thread_conn.execute(
                        f"INSERT INTO {table_name} VALUES ({placeholders})",
                        tuple(rec.values()),
                    )
                except Exception as e:
                    self._logger.error(f"Error inserting record {rec}: {e}")
                finally:
                    with progress_lock:
                        inserted_count += 1
                        progress = (inserted_count / total_valid) * 100
                        sys.stdout.write(f"\rInsert progress: {inserted_count}/{total_valid} ({progress:.1f}%)")
                        sys.stdout.flush()
            thread_conn.commit()  # Se o commit for necessário
            thread_conn.close()

        # Executa os batches em paralelo utilizando ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(insert_batch, batch) for batch in batches]
            concurrent.futures.wait(futures)

        sys.stdout.write("\n")
        self._logger.info(f"Completed inserting {total_valid} records")
