from pathlib import Path
from typing import Union, Dict, Optional, Any
import pandas as pd

from utils.base_processor import BaseProcessor
from ..core.indicators import Indicator
from ..core.validations import ValidationHelper
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager


class FeedbackProcessor(BaseProcessor):
    """
    Processor for extracting and validating competency data from Excel files.
    """

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsx"])

    def process_file(self, file_path: Union[str, Path]) -> Dict[str, Dict]:
        """
        Processes a single file to extract competency data.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.

        Returns:
            CompetencyMatrix: Processed competency data structured by evaluatee and evaluator.
        """
        competency_matrix: Dict[str, Dict] = {}
        evaluator_name = self._extract_evaluator_name(FileManager.get_file_name(file_path))

        excel_data = ExcelManager.read_excel(file_path)
        for sheet_name in excel_data.sheet_names[1:]:
            df = excel_data.parse(sheet_name)
            evaluatee_name = sheet_name.strip()
            self.logger.debug(f"Processing sheet: {sheet_name} for evaluatee: {evaluatee_name}")
            self.process_sheet(df, evaluatee_name, evaluator_name, competency_matrix)

        ValidationHelper.validate_competency_matrix(competency_matrix)
        self.logger.info(f"Validation completed for file: {file_path}")
        return competency_matrix

    def process_sheet(
        self,
        sheet_data: pd.DataFrame,
        evaluatee: str,
        evaluator: str,
        competency_matrix: Dict[str, Dict],
    ):
        """
        Processes a single sheet from an Excel file.

        Args:
            sheet_data (pd.DataFrame): DataFrame containing sheet data.
            evaluatee (str): Name of the person being evaluated.
            evaluator (str): Name of the evaluator.
            competency_matrix (CompetencyMatrix): Dictionary to store processed data.
        """
        if evaluatee not in competency_matrix:
            competency_matrix[evaluatee] = {}

        evaluator_data = competency_matrix[evaluatee].setdefault(evaluator, {})
        last_criterion = None

        for _, row in sheet_data.iterrows():
            criterion, indicator_data = self._process_row(row, last_criterion)
            if criterion:
                last_criterion = criterion
                if indicator_data:
                    evaluator_data.setdefault(criterion, []).append(indicator_data)

    def _process_row(self, row: pd.Series, last_criterion: Optional[str]) -> Optional[Indicator]:
        """
        Processes a single row from a sheet.

        Args:
            row (pd.Series): Row data.
            last_criterion (Optional[str]): Previously processed criterion.

        Returns:
            Optional[Indicator]: Processed indicator.
        """
        criterion = self._validate_field(row.iloc[0])
        if criterion == "Criteria":
            return None, None

        criterion = criterion or last_criterion
        if not criterion:
            return None, None

        indicator = self._validate_field(row.iloc[1])
        level = self._validate_field(row.iloc[2], allow_digits=True)
        evidence = self._validate_field(row.iloc[3])

        if indicator and level is not None:
            return (
                criterion,
                Indicator(name=indicator, level=level, evidence=evidence),
            )

        return criterion, None

    def _extract_evaluator_name(self, file_name: str) -> str:
        """
        Extracts evaluator name from Excel file name.

        Args:
            file_name (str): Name of the Excel file.

        Returns:
            str: Extracted evaluator name.
        """
        return file_name.split(" - ")[1].replace(".xlsx", "").strip()

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
