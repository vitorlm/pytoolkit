from pathlib import Path
from typing import Any

import pandas as pd

from utils.base_processor import BaseProcessor
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager

from ..core.indicators import Indicator
from ..core.validations import ValidationHelper


class FeedbackProcessor(BaseProcessor):
    """Processor for extracting and validating competency data from Excel files."""

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsx"])

    def process_file(self, file_path: str | Path, **kwargs) -> dict[str, Any]:
        """Processes a single file to extract competency data.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.
            **kwargs: Additional keyword arguments including:
                - period_metadata (dict): Optional metadata about evaluation period
                  (year, period_name, timestamp)

        Returns:
            Dict[str, Any]: Processed competency data structured by evaluatee and evaluator.
        """
        competency_matrix: dict[str, dict] = {}
        evaluator_name = self._extract_evaluator_name(FileManager.get_file_name(str(file_path)))

        # Extract period metadata if provided
        period_metadata = kwargs.get("period_metadata", None)

        excel_data = ExcelManager.read_excel(str(file_path))
        for sheet_name in excel_data.sheet_names[1:]:
            df = pd.read_excel(str(file_path), sheet_name=sheet_name)
            evaluatee_name = sheet_name.strip()

            # Handle self-evaluation: if sheet is "Self-Evaluation", use evaluator name as evaluatee
            if evaluatee_name.lower() == "self-evaluation":
                evaluatee_name = evaluator_name
                self.logger.debug(f"Processing self-evaluation sheet → evaluatee: {evaluatee_name}")
            else:
                self.logger.debug(f"Processing sheet: {sheet_name} → evaluatee: {evaluatee_name}")

            self.process_sheet(
                df,
                evaluatee=evaluatee_name,
                evaluator=evaluator_name,
                competency_matrix=competency_matrix,
                period_metadata=period_metadata,
            )

        ValidationHelper.validate_competency_matrix(competency_matrix)
        self.logger.info(f"Validation completed for file: {file_path}")

        # Period metadata is already embedded in evaluator_data
        # No need to wrap the entire result
        return competency_matrix

    def process_sheet(self, sheet_data: pd.DataFrame, **kwargs):
        """Processes a single sheet from an Excel file.

        Args:
            sheet_data (pd.DataFrame): DataFrame containing sheet data.
            **kwargs: Keyword arguments containing evaluatee, evaluator, competency_matrix,
                     and optional period_metadata.
        """
        evaluatee: str | None = kwargs.get("evaluatee")
        evaluator: str | None = kwargs.get("evaluator")
        competency_matrix: dict[str, dict[str, Any]] | None = kwargs.get("competency_matrix")
        period_metadata: dict | None = kwargs.get("period_metadata")

        if not evaluatee or not evaluator or competency_matrix is None:
            raise ValueError("Missing required arguments: evaluatee, evaluator, or competency_matrix")

        if evaluatee not in competency_matrix:
            competency_matrix[evaluatee] = {}

        evaluator_data: dict[str, Any] = competency_matrix[evaluatee].setdefault(evaluator, {})

        # Add period metadata to evaluator data if provided
        if period_metadata:
            evaluator_data["_period_metadata"] = period_metadata

        last_criterion = None

        for _, row in sheet_data.iterrows():
            result = self._process_row(row, last_criterion)
            if result is not None:
                criterion, indicator_data = result
                if criterion:
                    last_criterion = criterion
                    if indicator_data:
                        evaluator_data.setdefault(criterion, []).append(indicator_data)

    def _process_row(self, row: pd.Series, last_criterion: str | None) -> tuple[str, Indicator | None] | None:
        """Processes a single row from a sheet.

        Args:
            row (pd.Series): Row data.
            last_criterion (Optional[str]): Previously processed criterion.

        Returns:
            Optional[tuple[str, Optional[Indicator]]]: Tuple of (criterion, indicator) or None if invalid row.
        """
        criterion = self._validate_field(row.iloc[0])
        if criterion == "Criteria":
            return None

        criterion = criterion or last_criterion
        if not criterion:
            return None

        indicator = self._validate_field(row.iloc[1])
        level = self._validate_field(row.iloc[2], allow_digits=True)
        evidence = self._validate_field(row.iloc[3])

        # Ensure proper types for Indicator creation
        if indicator and level is not None and isinstance(criterion, str):
            # Ensure indicator is a string
            indicator_str = str(indicator) if not isinstance(indicator, str) else indicator
            # Ensure level is an integer
            level_int = int(level) if not isinstance(level, int) else level
            # Ensure evidence is a string or None
            evidence_str = str(evidence) if evidence is not None and not isinstance(evidence, str) else evidence

            return (
                criterion,
                Indicator(name=indicator_str, level=level_int, evidence=evidence_str),
            )

        if isinstance(criterion, str):
            return criterion, None
        else:
            return None

    def _extract_evaluator_name(self, file_name: str) -> str:
        """Extracts evaluator name from Excel file name.

        Args:
            file_name (str): Name of the Excel file.

        Returns:
            str: Extracted evaluator name.
        """
        return file_name.split(" - ")[1].replace(".xlsx", "").strip()

    def _validate_field(self, field: Any, allow_digits: bool = False) -> str | int | None:
        """Validates and processes a single field value from the Excel data.

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
