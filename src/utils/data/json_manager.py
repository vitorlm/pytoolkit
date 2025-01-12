import json
import os
from typing import Any, Dict
from filelock import FileLock


class JSONManager:
    """
    JSON file-specific operations including reading, writing, appending, and deleting.

    Example Usage:
        >>> JSONManager.read_json("example.json", default={})
        {}

        >>> JSONManager.write_json({"key": "value"}, "example.json")
        True
    """

    @staticmethod
    def read_json(file_path: str, default: Any = None) -> Any:
        """
        Reads and returns content from a JSON file. Returns a default value if file does not exist.

        Args:
            file_path (str): Path to the JSON file.
            default (Any): Value to return if the file is not found. Defaults to None.

        Returns:
            Any: Parsed content of the JSON file or the default value.
        """
        if not os.path.exists(file_path):
            if default is not None:
                return default
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def write_json(data: Any, file_path: str) -> bool:
        """
        Writes data to a JSON file. Creates a backup of the file if it already exists.

        Args:
            data (Any): Data to be written in JSON format.
            file_path (str): Path to save the JSON file.

        Returns:
            bool: True if the operation is successful.
        """
        lock = FileLock(f"{file_path}.lock")
        with lock:
            if os.path.exists(file_path):
                backup_path = f"{file_path}.bak"
                os.replace(file_path, backup_path)
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
        return True

    @staticmethod
    def append_or_update_json(file_path: str, updates: Dict) -> bool:
        """
        Appends or updates JSON data without overwriting the entire file.

        Args:
            file_path (str): Path to the JSON file.
            updates (Dict): Key-value pairs to append or update in the JSON file.

        Returns:
            bool: True if the operation is successful.
        """
        lock = FileLock(f"{file_path}.lock")
        with lock:
            if os.path.exists(file_path):
                existing_data = JSONManager.read_json(file_path, default={})
                if not isinstance(existing_data, dict):
                    raise ValueError("Existing JSON content must be a dictionary.")
                existing_data.update(updates)
            else:
                existing_data = updates

            JSONManager.write_json(existing_data, file_path)
        return True

    @staticmethod
    def delete_json(file_path: str) -> bool:
        """
        Deletes a JSON file.

        Args:
            file_path (str): Path to the JSON file to be deleted.

        Returns:
            bool: True if the operation is successful.
        """
        lock = FileLock(f"{file_path}.lock")
        with lock:
            if os.path.exists(file_path):
                os.remove(file_path)
                return True
            raise FileNotFoundError(f"JSON file not found: {file_path}")
