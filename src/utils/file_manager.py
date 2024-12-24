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
        Lista todos os arquivos em um diretório com um filtro opcional por extensão.

        Args:
            directory (str): Caminho do diretório.
            extension (Optional[str]): Extensão dos arquivos para filtrar (ex.: ".json").

        Returns:
            List[str]: Lista de caminhos dos arquivos que atendem ao filtro.

        Raises:
            FileNotFoundError: Se o diretório não existir.
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
        Lê o conteúdo de um arquivo.

        Args:
            file_path (str): Caminho do arquivo.

        Returns:
            List[str]: Conteúdo do arquivo como uma lista de linhas.

        Raises:
            FileNotFoundError: Se o arquivo não existir.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, "r", encoding="utf-8") as file:
            return file.readlines()

    @staticmethod
    def delete_file(file_path: str):
        """
        Deleta um arquivo.

        Args:
            file_path (str): Caminho do arquivo a ser deletado.

        Raises:
            FileNotFoundError: Se o arquivo não existir.
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
        timestamp_format: str = "%Y%m%d_%H%M%S",
        is_log_file: bool = False,
    ) -> str:
        """
        Generates a standardized and flexible file name.

        Args:
            module (Optional[str]): Name of the module or context (e.g., "logs", "reports").
            suffix (Optional[str]): Additional identifier or purpose for the file (e.g., "error", "summary").
            extension (str): File extension including the dot (e.g., ".log", ".json"). Default is ".txt".
            include_timestamp (bool): Whether to include a timestamp in the file name. Default is True.
            timestamp_format (str): Format for the timestamp, following `datetime.strftime` conventions.
            is_log_file (bool): If True, generates a log file name with a standard format.

        Returns:
            str: The generated file name.

        Raises:
            ValueError: If `extension` is invalid.
        """
        # If is_log_file is True, enforce standard log file naming
        if is_log_file:
            timestamp = datetime.datetime.now().strftime(timestamp_format)
            return f"log_{timestamp}.log"

        # Validate inputs
        if not extension.startswith("."):
            raise ValueError("Extension must start with a dot (e.g., '.txt').")

        # Sanitize inputs to avoid illegal characters in file names
        module = module.strip().replace(" ", "_") if module else None
        suffix = suffix.strip().replace(" ", "_") if suffix else None

        # Determine parts of the file name
        timestamp = (
            datetime.datetime.now().strftime(timestamp_format)
            if include_timestamp
            else ""
        )
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
    def create_folder(folder_path: str, exist_ok: Optional[bool] = True) -> None:
        """
        Creates a directory.

        Args:
            folder_path (str): Path of the folder to create.
            exist_ok (bool): If True, no error will be raised if the folder already exists. Default is True.

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
                raise OSError(
                    f"Failed to delete directory '{folder_path}' recursively: {e}"
                )
        else:
            try:
                if force:
                    # Remove all files in the folder
                    for file_name in os.listdir(folder_path):
                        file_path = os.path.join(folder_path, file_name)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                        elif os.path.isdir(file_path):
                            raise OSError(
                                f"Subdirectory found in non-recursive mode: {file_path}"
                            )
                os.rmdir(folder_path)
            except OSError as e:
                raise OSError(f"Failed to delete directory '{folder_path}': {e}")
