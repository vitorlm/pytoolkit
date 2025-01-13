import datetime
import os
from typing import List, Optional, Dict


class FileManager:
    """
    General file management operations including listing, reading, deleting files,
    and retrieving metadata.
    """

    @staticmethod
    def list_files(directory: str, extension: Optional[str] = None) -> List[str]:
        """
        Lists all files in a directory with an optional filter by extension.

        Args:
            directory (str): Path to the directory.
            extension (Optional[str]): File extension to filter by (e.g., ".json").

        Returns:
            List[str]: List of file paths that match the filter.

        Raises:
            FileNotFoundError: If the directory does not exist.
        """
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory not found: {directory}")
        return [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, f))
            and (extension is None or f.endswith(extension))
        ]

    @staticmethod
    def read_file(file_path: str) -> List[str]:
        """
        Reads the content of a file.

        Args:
            file_path (str): Path to the file.

        Returns:
            List[str]: Content of the file as a list of lines.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as file:
            return file.readlines()

    @staticmethod
    def delete_file(file_path: str) -> None:
        """
        Deletes a file.

        Args:
            file_path (str): Path to the file to be deleted.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            raise FileNotFoundError(f"File not found: {file_path}")

    @staticmethod
    def batch_delete_files(file_paths: List[str]) -> None:
        """
        Deletes multiple files at once.

        Args:
            file_paths (List[str]): List of file paths to delete.

        Raises:
            FileNotFoundError: If any file in the list does not exist.
        """
        for file_path in file_paths:
            FileManager.delete_file(file_path)

    @staticmethod
    def generate_file_name(
        module: Optional[str] = None,
        suffix: Optional[str] = None,
        extension: str = ".txt",
        include_timestamp: bool = True,
        timestamp_format: str = "%Y%m%d%H%M%S",
    ) -> str:
        """
        Generates a standardized and flexible file name.

        Args:
            module (Optional[str]): Module or context name.
            suffix (Optional[str]): Additional identifier.
            extension (str): File extension (e.g., ".log").
            include_timestamp (bool): Whether to include a timestamp.
            timestamp_format (str): Format for the timestamp.

        Returns:
            str: The generated file name.
        """
        if not extension.startswith("."):
            raise ValueError("Extension must start with a dot.")
        parts = [
            module.replace(" ", "_") if module else None,
            datetime.datetime.now().strftime(timestamp_format) if include_timestamp else None,
            suffix.replace(" ", "_") if suffix else None,
        ]
        return "_".join(filter(None, parts)) + extension

    @staticmethod
    def retrieve_metadata(file_path: str) -> Dict[str, str]:
        """
        Retrieves metadata of a file.

        Args:
            file_path (str): Path to the file.

        Returns:
            Dict[str, str]: File metadata including size, creation, and modification date.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        stats = os.stat(file_path)
        return {
            "size": f"{stats.st_size} bytes",
            "created": datetime.datetime.fromtimestamp(stats.st_ctime).isoformat(),
            "modified": datetime.datetime.fromtimestamp(stats.st_mtime).isoformat(),
        }

    @staticmethod
    def create_folder(folder_path: str, exist_ok: bool = True) -> None:
        """
        Creates a folder.

        Args:
            folder_path (str): Path of the folder to create.
            exist_ok (bool): If True, suppresses errors if the folder exists.

        Raises:
            OSError: If the folder cannot be created.
        """
        os.makedirs(folder_path, exist_ok=exist_ok)

    @staticmethod
    def validate_file(file_path: str, allowed_extensions: Optional[List[str]] = None) -> None:
        """
        Validates file existence and extension.

        Args:
            file_path (str): Path to the file.
            allowed_extensions (Optional[List[str]]): Valid extensions.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file extension is invalid.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        if allowed_extensions:
            _, ext = os.path.splitext(file_path)
            if ext.lower() not in allowed_extensions:
                raise ValueError(f"Invalid file extension: {ext}. Allowed: {allowed_extensions}")

    @staticmethod
    def is_folder(path: str) -> bool:
        """
        Checks if the given path is a folder.

        Args:
            path (str): The path to validate.

        Returns:
            bool: True if the path is a folder, False otherwise.
        """
        return os.path.isdir(path)

    @staticmethod
    def validate_folder(folder_path: str) -> None:
        """
        Validates if a folder exists.

        Args:
            folder_path (str): Path to the folder to validate.

        Raises:
            FileNotFoundError: If the folder does not exist.
        """
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

    @staticmethod
    def get_file_name(file_name: str) -> str:
        """
        Extracts the module name from a file name.

        Args:
            file_name (str): Name of the file.

        Returns:
            str: Module name extracted from the file name.
        """
        return os.path.splitext(os.path.basename(file_name))[0]

    @staticmethod
    def file_exists(file_path: str) -> bool:
        """
        Checks if a file exists.

        Args:
            file_path (str): Path to the file.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        return os.path.isfile(file_path)
