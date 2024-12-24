import json
import os
from typing import Any, Dict


class JSONManager:
    """
    JSON file-specific operations including reading, writing, appending, and deleting.
    """

    @staticmethod
    def read_json(file_path: str) -> Any:
        """
        Reads and returns content from a JSON file.

        Args:
            file_path (str): Path to the JSON file.

        Returns:
            Any: Parsed content of the JSON file.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the file is not a valid JSON.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"JSON file not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def write_json(data: Any, file_path: str) -> None:
        """
        Writes data to a JSON file.

        Args:
            data (Any): Data to be written in JSON format.
            file_path (str): Path to save the JSON file.
        """
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

    @staticmethod
    def append_or_update_json(file_path: str, updates: Dict) -> None:
        """
        Appends or updates JSON data without overwriting the entire file.

        Args:
            file_path (str): Path to the JSON file.
            updates (Dict): Key-value pairs to append or update in the JSON file.

        Raises:
            FileNotFoundError: If the file does not exist.
            json.JSONDecodeError: If the existing file is not a valid JSON.
        """
        if os.path.exists(file_path):
            try:
                existing_data = JSONManager.read_json(file_path)
                if not isinstance(existing_data, dict):
                    raise ValueError("Existing JSON content must be a dictionary.")
            except FileNotFoundError:
                existing_data = {}

            existing_data.update(updates)
        else:
            existing_data = updates

        JSONManager.write_json(existing_data, file_path)

    @staticmethod
    def delete_json(file_path: str) -> None:
        """
        Deletes a JSON file.

        Args:
            file_path (str): Path to the JSON file to be deleted.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            raise FileNotFoundError(f"JSON file not found: {file_path}")
