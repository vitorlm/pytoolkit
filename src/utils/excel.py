import os
from typing import List, Tuple, Union

import pandas as pd


def load_excel_files(
    folder_path: str, file_extension: str = ".xlsx"
) -> List[Tuple[str, pd.ExcelFile]]:
    """
    Loads all Excel files from the specified directory.

    Args:
        folder_path (str): Path to the folder containing Excel files.
        file_extension (str): File extension to filter (default is '.xlsx').

    Returns:
        List[Tuple[str, pd.ExcelFile]]: A list of tuples with file names and their respective Excel objects.
    """
    excel_files = []
    for file_name in os.listdir(folder_path):
        if file_name.endswith(file_extension):
            file_path = os.path.join(folder_path, file_name)
            excel_files.append((file_name, pd.ExcelFile(file_path)))
    return excel_files


def read_excel(file_path: str, sheet_name: Union[str, int] = 0) -> pd.DataFrame:
    """
    Reads a specific sheet from an Excel file.

    Args:
        file_path (str): Path to the Excel file.
        sheet_name (Union[str, int]): Name or index of the sheet to read (default is 0).

    Returns:
        pd.DataFrame: The content of the sheet as a DataFrame.
    """
    return pd.read_excel(file_path, sheet_name=sheet_name)


def write_to_excel(data: pd.DataFrame, output_path: str, sheet_name: str = "Sheet1"):
    """
    Writes a DataFrame to an Excel file.

    Args:
        data (pd.DataFrame): The data to write to Excel.
        output_path (str): Path to save the Excel file.
        sheet_name (str): Name of the sheet to write to (default is 'Sheet1').
    """
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        data.to_excel(writer, index=False, sheet_name=sheet_name)


def list_sheets(file_path: str) -> List[str]:
    """
    Lists all sheet names in an Excel file.

    Args:
        file_path (str): Path to the Excel file.

    Returns:
        List[str]: A list of sheet names.
    """
    return pd.ExcelFile(file_path).sheet_names
