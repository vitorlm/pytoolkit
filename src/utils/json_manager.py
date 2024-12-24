import json
import os
from typing import Any


class JSONManager:
    """
    JSON file-specific operations including reading, writing, and deleting.
    """

    @staticmethod
    def read_json(file_path: str) -> Any:
        """
        Read and return content from a JSON file.

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
    def write_json(data: Any, file_path: str):
        """
        Write data to a JSON file.

        Args:
            data (Any): Data to be written in JSON format.
            file_path (str): Path to save the JSON file.
        """
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
