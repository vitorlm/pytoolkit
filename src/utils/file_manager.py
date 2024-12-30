import datetime
import os
import shutil
from typing import List, Optional


class FileManager:
    """
    General file management operations including listing and deleting files.
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
    def delete_file(file_path: str):
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
    def generate_file_name(
        module: Optional[str] = None,
        suffix: Optional[str] = None,
        extension: str = ".txt",
        include_timestamp: bool = True,
        timestamp_format: str = "%Y%m%d%H%M%S",
        is_log_file: bool = False,
    ) -> str:
        """
        Generates a standardized and flexible file name.

        Args:
            module (Optional[str]): Name of the module or context (e.g., "logs", "reports").
            suffix (Optional[str]): Additional identifier or purpose for the file
                                   (e.g., "error", "summary").
            extension (str): File extension including the dot (e.g., ".log", ".json").
                             Default is ".txt".
            include_timestamp (bool): Whether to include a timestamp in the file name.
                                      Default is True.
            timestamp_format (str): Format for the timestamp, following `datetime.strftime`
                                    conventions.
                                    Default is "%Y%m%d%H%M%S".
            is_log_file (bool): If True, generates a log file name with a standard format.

        Returns:
            str: The generated file name.

        Raises:
            ValueError: If `extension` is invalid.
        """
        # If is_log_file is True, enforce standard log file naming
        if is_log_file:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"log_{timestamp}.log"

        # Validate inputs
        if not extension.startswith("."):
            raise ValueError("Extension must start with a dot (e.g., '.txt').")

        # Sanitize inputs to avoid illegal characters in file names
        module = module.strip().replace(" ", "_") if module else None
        suffix = suffix.strip().replace(" ", "_") if suffix else None

        # Determine parts of the file name
        timestamp = datetime.datetime.now().strftime(timestamp_format) if include_timestamp else ""
        parts = []
        if module:
            parts.append(module)
        if timestamp:
            parts.append(timestamp)
        if suffix:
            parts.append(suffix)

        # Join parts and append the extension
        return "_".join(parts) + extension

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
    def create_folder(folder_path: str, exist_ok: Optional[bool] = True) -> None:
        """
        Creates a directory.

        Args:
            folder_path (str): Path of the folder to create.
            exist_ok (bool): If True, no error will be raised if the folder already exists.
                             Default is True.

        Raises:
            OSError: If the directory cannot be created.
        """
        try:
            os.makedirs(folder_path, exist_ok=exist_ok)
        except OSError as e:
            raise OSError(f"Failed to create directory '{folder_path}': {e}")

    @staticmethod
    def delete_folder(
        folder_path: str,
        recursive: Optional[bool] = False,
        force: Optional[bool] = False,
    ) -> None:
        """
        Deletes a directory.

        Args:
            folder_path (str): Path of the folder to delete.
            recursive (bool): If True, delete the folder and all its contents. Default is False.
            force (bool): If True, delete even if the folder contains files. Default is False.

        Raises:
            FileNotFoundError: If the folder does not exist.
            OSError: If the folder cannot be deleted.
        """
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        if recursive:
            try:
                shutil.rmtree(folder_path)
            except OSError as e:
                raise OSError(f"Failed to delete directory '{folder_path}' recursively: {e}")
        else:
            try:
                if force:
                    # Remove all files in the folder
                    for file_name in os.listdir(folder_path):
                        file_path = os.path.join(folder_path, file_name)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            raise OSError(f"Subdirectory found in non-recursive mode: {file_path}")
                os.rmdir(folder_path)
            except OSError as e:
                raise OSError(f"Failed to delete directory '{folder_path}': {e}")

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
    def validate_file(file_path: str, allowed_extensions: Optional[List[str]] = None) -> None:
        """
        Validates if a file exists and optionally checks for allowed extensions.

        Args:
            file_path (str): Path to the file to validate.
            allowed_extensions (Optional[List[str]]): List of allowed extensions.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file's extension is not allowed.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        if allowed_extensions:
            _, ext = os.path.splitext(file_path)
            if ext.lower() not in allowed_extensions:
                raise ValueError(f"Invalid file extension: {ext}. Allowed: {allowed_extensions}")

    @staticmethod
    def get_module_name(file_name: str) -> str:
        """
        Extracts the module name from a file name.

        Args:
            file_name (str): Name of the file.

        Returns:
            str: Module name extracted from the file name.
        """
        return os.path.splitext(os.path.basename(file_name))[0]
