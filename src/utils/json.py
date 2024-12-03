import json
import os
from typing import Any


def read_json(file_path: str) -> Any:
    """
    Reads a JSON file and returns its content.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        Any: Parsed JSON data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"JSON file not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as json_file:
        return json.load(json_file)


def write_json(data: Any, output_path: str):
    """
    Writes data to a JSON file.

    Args:
        data (Any): Data to save in JSON format.
        output_path (str): Path to save the JSON file.
    """
    with open(output_path, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4, ensure_ascii=False)


def delete_json(file_path: str):
    """
    Deletes a JSON file.

    Args:
        file_path (str): Path to the JSON file to delete.
    """
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        raise FileNotFoundError(f"JSON file not found: {file_path}")
