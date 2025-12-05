import re
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from utils.base_processor import BaseProcessor
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager


class Criterion(BaseModel):
    """Model for storing criterion details including its indicators and levels."""

    name: str
    indicators: dict[str, dict]


class CriteriaProcessor(BaseProcessor):
    """Processor for extracting criteria, indicators, and levels from an Excel file."""

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsm", ".xlsx"])

    def process_file(self, file_path: Path) -> list[Criterion]:
        """Processes an Excel file to extract criteria with their indicators and levels.

        Args:
            file_path (Path): Path to the Excel file.

        Returns:
            List[Criterion]: A list of extracted criteria with their respective indicators
            and levels.
        """
        file_path = Path(file_path)
        FileManager.validate_file(file_path, allowed_extensions=self.allowed_extensions)

        sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name="Competencies and Levels")
        criteria = self.process_sheet(sheet_data)
        return criteria

    def _extract_description_and_evidence(self, text):
        # Expressão regular para capturar a descrição e a evidência sugerida (se existir)
        match = re.search(r"^(.*?)\s*(?:\r?\n\r?\n)?Suggested evidence:\s*(.*)", text, re.DOTALL)

        if match:
            description = match.group(1).strip()
            suggested_evidence = match.group(2).strip()
        else:
            description = text.strip()
            suggested_evidence = None

        return {"description": description, "suggested_evidence": suggested_evidence}

    def process_sheet(self, sheet_data: list[list[str | None]]) -> list[Criterion]:
        """Extracts criteria, indicators, and levels from the sheet data.

        Args:
            sheet_data (List[List[Optional[str]]]): The data from the Competencies and Levels sheet.

        Returns:
            List[Criterion]: A list of criteria with detailed indicators and levels.
        """
        criteria = []
        criterion_dict = {}

        last_criterion = None
        for row in sheet_data[1:]:
            criterion_name = row[0] if isinstance(row[0], str) and row[0].strip() else None
            indicator_name = row[1] if isinstance(row[1], str) and row[1].strip() else None
            if indicator_name is None:
                continue

            if criterion_name is None:
                criterion_name = last_criterion

            levels = {i + 1: self._extract_description_and_evidence(row[i + 2]) for i in range(5)}

            if pd.notna(criterion_name):
                if criterion_name not in criterion_dict:
                    criterion_dict[criterion_name] = {}

            if pd.notna(indicator_name):
                criterion_dict[criterion_name][indicator_name] = {
                    "name": indicator_name,
                    "levels": levels,
                }
            last_criterion = criterion_name

        for criterion_name, indicators in criterion_dict.items():
            criteria.append(Criterion(name=criterion_name, indicators=indicators))

        return criteria
