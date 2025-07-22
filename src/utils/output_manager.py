import os
from typing import Optional
from utils.file_manager import FileManager


class OutputManager:
    """Utility class for managing output file paths in the project."""

    PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")

    @staticmethod
    def get_output_path(
        command_name: str, filename: Optional[str] = None, extension: str = ".json"
    ) -> str:
        """
        Generate a standardized output path for a command.

        Args:
            command_name (str): Name of the command (used as sub-folder name)
            filename (str, optional): Specific filename. If None, generates a timestamped name
            extension (str): File extension (default: .json)

        Returns:
            str: Full path to the output file
        """
        # Create command-specific subdirectory
        command_output_dir = os.path.join(OutputManager.OUTPUT_DIR, command_name)
        FileManager.create_folder(command_output_dir)

        # Generate filename if not provided
        if filename is None:
            filename = FileManager.generate_file_name(
                module=command_name, suffix="output", extension=extension
            )
        elif not filename.endswith(extension):
            filename = f"{filename}{extension}"

        return os.path.join(command_output_dir, filename)

    @staticmethod
    def ensure_output_dir():
        """Ensure the main output directory exists."""
        FileManager.create_folder(OutputManager.OUTPUT_DIR)
