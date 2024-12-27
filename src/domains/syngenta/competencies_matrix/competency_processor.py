from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
from pydantic import BaseModel

from log_config import log_manager
from utils.excel_manager import ExcelManager
from utils.file_manager import FileManager

# Type aliases for better readability
CompetencyMatrix = Dict[str, Dict]


class Indicator(BaseModel):
    """
    Model for validating indicator-level competency data.

    Attributes:
        indicator (str): Name or description of the competency indicator.
        level (int): Numerical level achieved for this indicator.
        evidence (Optional[str]): Optional supporting evidence or comments.
    """

    indicator: str
    level: int
    evidence: Optional[str]


class CompetencyProcessor:
    """
    Processor for extracting and validating competency data from Excel files.
    """

    def __init__(self):
        """
        Initializes the CompetencyProcessor with logging.
        """
        self.logger = log_manager.get_logger(module_name=FileManager.get_module_name(__file__))

    def process_folder(self, folder_path: Union[str, Path]) -> CompetencyMatrix:
        """
        Processes all Excel files in the specified folder for competency data.

        Args:
            folder_path (Union[str, Path]): Directory containing competency Excel files.

        Returns:
            CompetencyMatrix: Dictionary containing processed competency data by evaluator.

        Raises:
            FileNotFoundError: If the specified folder does not exist.
            ValueError: If any file contains invalid data.
        """
        folder_path = Path(folder_path)
        self.logger.info(f"Loading Excel files from {folder_path}")

        # Validate folder path
        FileManager.validate_folder(folder_path)

        excel_files = ExcelManager.load_multiple_excel_files(folder_path)
        competency_matrix: CompetencyMatrix = {}

        for file_name, excel_data in excel_files:
            try:
                self.logger.debug(f"Processing file: {file_name}")
                self._process_file(file_name, excel_data, competency_matrix)
            except ValueError as e:
                self.logger.error(f"Error processing file '{file_name}': {e}", exc_info=True)
                raise

        return competency_matrix

    def _process_file(
        self,
        file_name: str,
        excel_data: pd.ExcelFile,
        competency_matrix: CompetencyMatrix,
    ) -> None:
        """
        Processes a single Excel file for competency data.

        Args:
            file_name (str): Name of the Excel file being processed.
            excel_data (pd.ExcelFile): Loaded Excel file data.
            competency_matrix (CompetencyMatrix): Dictionary to store processed data.
        """
        evaluator_name = self._extract_evaluator_name(file_name)
        self.logger.info(f"Processing evaluator: {evaluator_name}")

        for sheet_name in excel_data.sheet_names[1:]:
            df = excel_data.parse(sheet_name)
            evaluated_name = sheet_name.strip()
            self._process_sheet(df, evaluated_name, evaluator_name, competency_matrix)

    def _extract_evaluator_name(self, file_name: str) -> str:
        """
        Extracts evaluator name from Excel file name.

        Args:
            file_name (str): Name of the Excel file.

        Returns:
            str: Extracted evaluator name.
        """
        return file_name.split(" - ")[1].replace(".xlsx", "").strip()

    def _process_sheet(
        self,
        df: pd.DataFrame,
        evaluated_name: str,
        evaluator_name: str,
        competency_matrix: CompetencyMatrix,
    ) -> None:
        """
        Processes a single sheet from an Excel file.

        Args:
            df (pd.DataFrame): DataFrame containing sheet data.
            evaluated_name (str): Name of the person being evaluated.
            evaluator_name (str): Name of the person providing evaluation.
            competency_matrix (CompetencyMatrix): Dictionary to store processed data.
        """
        if evaluated_name not in competency_matrix:
            competency_matrix[evaluated_name] = {}

        evaluator_data = competency_matrix[evaluated_name].setdefault(evaluator_name, {})
        last_criteria = None

        for _, row in df.iterrows():
            criteria, indicator_data = self._process_row(row, last_criteria)
            if criteria:
                last_criteria = criteria
                if indicator_data:
                    evaluator_data.setdefault(criteria, []).append(indicator_data)

    def _validate_field(self, field: Any, allow_digits: bool = False) -> Optional[Union[str, int]]:
        """
        Validates and processes a single field value from the Excel data.

        Args:
            field (Any): The value to validate.
            allow_digits (bool): Whether to convert string digits to integers.

        Returns:
            Optional[Union[str, int]]: Processed field value or None if invalid.
        """
        if pd.isna(field) or field == "":
            return None

        if isinstance(field, str):
            field = field.strip()
            if allow_digits and field.isdigit():
                return int(field)
            return field

        if isinstance(field, (int, float)) and allow_digits:
            return int(field)

        return None

    def _process_row(
        self, row: pd.Series, last_criteria: Optional[str]
    ) -> Tuple[Optional[str], Optional[Dict]]:
        """
        Processes a single row of competency data.

        Args:
            row (pd.Series): Series containing row data.
            last_criteria (Optional[str]): Previously processed criteria name.

        Returns:
            Tuple[Optional[str], Optional[Dict]]: Tuple of criteria name and processed
            indicator data.
        """
        criteria = self._validate_field(row.iloc[0])
        if criteria == "Criteria":
            return None, None

        criteria = criteria or last_criteria
        if not criteria:
            return None, None

        indicator = self._validate_field(row.iloc[1])
        level = self._validate_field(row.iloc[2], allow_digits=True)
        evidence = self._validate_field(row.iloc[3])

        if indicator and level is not None:
            return (
                criteria,
                Indicator(indicator=indicator, level=level, evidence=evidence).dict(),
            )

        return criteria, None
