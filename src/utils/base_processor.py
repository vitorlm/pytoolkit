import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager

# Suppress openpyxl Data Validation warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


class BaseProcessor(ABC):
    """Base class for processing data from files and directories."""

    def __init__(self, allowed_extensions: list[str] | None = None):
        """Initializes the BaseProcessor with optional file extensions and a logger.

        Args:
            allowed_extensions (Optional[List[str]]): List of allowed file extensions.
                If None, all file types are allowed.
        """
        self.logger = LogManager.get_instance().get_logger(self.__class__.__name__)

        # Set allowed file extensions
        self.allowed_extensions = allowed_extensions or ["*"]

    def process_folder(self, folder_path: str | Path, **kwargs) -> dict[str, Any]:
        """Processes all files in a given folder with additional parameters.

        Args:
            folder_path (Union[str, Path]): Path to the folder.
            **kwargs: Additional parameters to pass to the file processing method.

        Returns:
            Dict[str, Any]: Aggregated data from all processed files.
        """
        folder_path = Path(folder_path)
        self.logger.info(f"Processing folder using {self.__class__.__name__}: {folder_path}")

        # Validate the folder
        FileManager.validate_folder(folder_path)

        data = {}
        for file_path in folder_path.glob("*.*"):
            if not self._is_allowed_extension(file_path):
                self.logger.warning(f"Skipping file with unsupported extension: {file_path}")
                continue

            try:
                self.logger.info(f"Processing file: {file_path} with {self.__class__.__name__}")
                file_data = self.process_file(file_path, **kwargs)

                for key, value in file_data.items():
                    if isinstance(value, dict) and isinstance(data.get(key), dict):
                        data[key].update(value)
                    else:
                        data[key] = value
            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}", exc_info=True)

        return data

    @abstractmethod
    def process_file(self, file_path: str | Path, **kwargs) -> dict[str, Any]:
        """Processes a single file.

        Args:
            file_path (Union[str, Path]): Path to the file.

        Returns:
            Dict[str, Any]: Data extracted from the file.
        """
        pass

    @abstractmethod
    def process_sheet(self, sheet_data: Any, **kwargs) -> Any:
        """Processes a single sheet of data.

        Args:
            sheet_data (Any): Data from a sheet.

        Returns:
            Any: Processed sheet data.
        """
        pass

    def _is_allowed_extension(self, file_path: str | Path) -> bool:
        """Checks if a file has an allowed extension.

        Args:
            file_path (Union[str, Path]): Path to the file.

        Returns:
            bool: True if the file's extension is allowed, False otherwise.
        """
        if "*" in self.allowed_extensions:
            return True
        return file_path.suffix in self.allowed_extensions
