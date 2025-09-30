import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager


class OutputManager:
    _output_dir = "output"

    @staticmethod
    def get_output_root() -> str:
        """Return the base output directory used for generated artifacts."""
        return OutputManager._output_dir

    @staticmethod
    def get_output_path(sub_dir: str, file_name: str, extension: str = "json") -> str:
        """
        Constructs a standardized file path within the output directory,
        ensuring the subdirectory exists.

        Args:
            sub_dir (str): The subdirectory within the main output folder (e.g., 'net-flow').
            file_name (str): The base name of the file, without timestamp or extension.
            extension (str): The file extension (default: 'json').

        Returns:
            str: The full, standardized path to the output file.
        """
        # Create a timestamped and formatted file name
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_file_name = f"{file_name}_{timestamp}.{extension}"

        # Create the full path
        target_dir = os.path.join(OutputManager._output_dir, sub_dir)
        os.makedirs(target_dir, exist_ok=True)

        return os.path.join(target_dir, full_file_name)

    @staticmethod
    def save_json_report(
        data: Dict, sub_dir: str, file_basename: str, output_path: Optional[str] = None
    ) -> str:
        """
        Saves a dictionary as a JSON report.

        Args:
            data (Dict): The dictionary data to save.
            sub_dir (str): The subdirectory for the report (e.g., 'net-flow').
            file_basename (str): The base name for the file.
            output_path (Optional[str]): Optional custom full path to save the file.

        Returns:
            str: The path where the file was saved.
        """
        if output_path:
            path = output_path
        else:
            path = OutputManager.get_output_path(sub_dir, file_basename, "json")

        # Ensure directory exists for the given path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        JSONManager.write_json(data, path)
        return path

    @staticmethod
    def save_markdown_report(
        content: str,
        sub_dir: str,
        file_basename: str,
        output_path: Optional[str] = None,
    ) -> str:
        """
        Saves a string as a Markdown report.

        Args:
            content (str): The Markdown content to save.
            sub_dir (str): The subdirectory for the report (e.g., 'net-flow').
            file_basename (str): The base name for the file.
            output_path (Optional[str]): Optional custom full path to save the file.

        Returns:
            str: The path where the file was saved.
        """
        if output_path:
            path = output_path
        else:
            path = OutputManager.get_output_path(sub_dir, file_basename, "md")

        # Ensure directory exists for the given path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

        FileManager.write_file(path, content)
        return path

    @staticmethod
    def save_summary_report(
        metrics: List[Dict[str, Any]],
        sub_dir: str,
        file_basename: str,
        output_path: Optional[str] = None,
    ) -> str:
        """Persist summary metrics using the standard output directory layout."""
        if output_path:
            path = output_path
        else:
            path = OutputManager.get_output_path(sub_dir, file_basename, "json")

        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        JSONManager.write_json(metrics, path)
        return path
