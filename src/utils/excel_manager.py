import os
import re
from typing import List, Optional, Tuple, Union

import pandas as pd


class ExcelManager:
    """
    Excel file-specific operations including reading, writing, and listing sheets.
    """

    @staticmethod
    def read_excel(file_path: str) -> pd.DataFrame:
        """
        Reads an Excel file and returns the specified sheet as a DataFrame.

        Args:
            file_path (str): Path to the Excel file.
            sheet_name (Optional[str]): Name of the sheet to read. If None, the first sheet is read.

        Returns:
            pd.DataFrame: Data from the specified sheet.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the specified sheet does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found: {file_path}")

        return pd.ExcelFile(file_path, engine="calamine")

    @staticmethod
    def write_excel(data: pd.DataFrame, file_path: str, sheet_name: str = "Sheet1") -> None:
        """
        Writes a DataFrame to an Excel file.

        Args:
            data (pd.DataFrame): Data to be written to Excel.
            file_path (str): Path to save the Excel file.
            sheet_name (str): Name of the sheet to write. Default is "Sheet1".
        """
        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            data.to_excel(writer, index=False, sheet_name=sheet_name)

    @staticmethod
    def list_excel_sheets(file_path: str) -> List[str]:
        """
        Lists all sheet names in an Excel file.

        Args:
            file_path (str): Path to the Excel file.

        Returns:
            List[str]: Names of all sheets in the file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel file not found: {file_path}")
        return pd.ExcelFile(file_path).sheet_names

    @staticmethod
    def load_multiple_excel_files(
        folder_path: str, file_extension: Optional[str] = ".xlsx"
    ) -> List[Tuple[str, pd.ExcelFile]]:
        """
        Loads all Excel files from the specified directory.

        Args:
            folder_path (str): Path to the folder containing Excel files.
            file_extension (str): File extension to filter (default is '.xlsx').

        Returns:
            List[Tuple[str, pd.ExcelFile]]: A list of tuples with file names and their
                                            respective Excel objects.

        Raises:
            FileNotFoundError: If the specified folder does not exist.
            ValueError: If no valid Excel files are found in the directory.
        """
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"The folder '{folder_path}' does not exist.")

        excel_files = []
        errors = []

        for file_name in os.listdir(folder_path):
            if file_name.endswith(file_extension):
                file_path = os.path.join(folder_path, file_name)
                try:
                    excel_files.append((file_name, pd.ExcelFile(file_path)))
                except Exception as e:
                    errors.append((file_name, str(e)))

        if errors:
            for file_name, error_message in errors:
                print(f"Error loading file '{file_name}': {error_message}")

        if not excel_files:
            raise ValueError(
                f"No valid Excel files found in '{folder_path}' with extension '{file_extension}'."
            )

        return excel_files

    @staticmethod
    def filter_sheets_by_pattern(file_path: str, pattern: str) -> List[str]:
        """
        Filters sheet names in an Excel file based on a regex pattern.
        Supports inclusion or exclusion patterns.

        Args:
            file_path (str): Path to the Excel file.
            pattern (str): Regex pattern to match sheet names.
                        Use negative lookahead for exclusions.

        Returns:
            List[str]: List of matching sheet names.
        """
        sheets = ExcelManager.list_excel_sheets(file_path)

        # Compile the regex pattern
        regex = re.compile(pattern, re.IGNORECASE)

        # Filter sheets based on the regex pattern
        return [sheet for sheet in sheets if regex.search(sheet)]

    @staticmethod
    def extract_range_by_indices(
        df: pd.DataFrame, start_row: int, end_row: int, start_col: int, end_col: int
    ) -> pd.DataFrame:
        """
        Extracts a range of data from a DataFrame using row and column indices.

        Args:
            df (pd.DataFrame): DataFrame to extract data from.
            start_row (int): Starting row index.
            end_row (int): Ending row index.
            start_col (int): Starting column index.
            end_col (int): Ending column index.

        Returns:
            pd.DataFrame: Extracted range as a new DataFrame.
        """
        return df.iloc[start_row:end_row, start_col:end_col].dropna(how="all")

    @staticmethod
    def get_non_empty_values(
        df: pd.DataFrame, row_start: int, col_start: int, col_end: int
    ) -> List[str]:
        """
        Extracts non-empty values from a specific row range in the DataFrame.

        Args:
            df (pd.DataFrame): DataFrame to extract from.
            row_start (int): Row start index.
            col_start (int): Column start index.
            col_end (int): Column end index.

        Returns:
            List[str]: List of non-empty values.
        """
        return df.iloc[row_start:, col_start:col_end].dropna(axis=1).values.flatten().tolist()

    @staticmethod
    def read_excel_as_list(file_path: str, sheet_name: str) -> List[List[Union[str, None]]]:
        """
        Reads an Excel sheet and returns its data as a list of lists.

        Args:
            file_path (str): Path to the Excel file.
            sheet_name (str): Name of the sheet to read.

        Returns:
            List[List[Union[str, None]]]: Sheet data as rows of values.
        """
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine="calamine")
        df_filled = df.fillna("")
        return df_filled.values.tolist()
