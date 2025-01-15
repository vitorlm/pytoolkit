from pathlib import Path
from typing import Dict, List, Union, Set

from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager
from ..core.config import Config
from utils.base_processor import BaseProcessor
from ..core.task import Task

# Configure logger
logger = LogManager.get_instance().get_logger("TeamTaskProcessor")


class TeamTaskProcessor(BaseProcessor):
    """
    Processor for extracting task/epic allocation from Excel files.
    """

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsm", ".xlsx"])
        self.config = Config()
        self.backlog = Set[Task]

    def process_file(self, file_path: Union[str, Path]) -> Dict[str, Set[Task]]:
        """
        Processes an Excel file to extract task allocations for team members.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.

        Returns:
            Dict[str, Set[Task]]: Task map with members as keys and their tasks as values.

        Raises:
            ValueError: If the file contains invalid data.
        """
        file_path = Path(file_path)

        # Validate file extension
        FileManager.validate_file(file_path, allowed_extensions=self.allowed_extensions)

        # Filter sheets to process based on naming convention
        relevant_sheets = ExcelManager.filter_sheets_by_pattern(file_path, pattern=r"Q[1-4]-C[1-2]")

        self.logger.info(f"Processing {len(relevant_sheets)} relevant sheets from {file_path}")
        for sheet_name in relevant_sheets:
            sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name=sheet_name)
            self.logger.info(f"Processing sheet: {sheet_name}")
            self.process_sheet(sheet_data)

        return self.task_map

    def process_sheet(self, sheet_data: List[List[Union[str, None]]]):
        """
        Processes a single sheet to map tasks to team members.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data as rows of values.
        """
        header_idxs = self._extract_header(sheet_data)
        # members = self._extract_members(sheet_data)
        self.backlog = self._create_backlog(sheet_data, header_idxs)
        self._extract_task_duration(sheet_data)

    def _extract_header(self, sheet_data: List[List[Union[str, None]]]) -> Dict[str, int]:
        """
        Extracts the header row from the sheet data and returns a dictionary
        mapping header items to their column indices.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, int]: A dictionary with header items as keys and column indices as values.
        """
        header_map = {}
        for row_idx in range(self.config.row_header_start, self.config.row_header_end):
            for col_idx, cell_value in enumerate(sheet_data[row_idx - 1]):
                if isinstance(cell_value, str) and cell_value.lower() in [
                    "code",
                    "jira",
                    "subject",
                    "type",
                ]:
                    header_map[cell_value.lower()] = col_idx
                    if len(header_map) == 4:
                        return header_map
        return header_map

    def _extract_members(self, sheet_data: List[List[Union[str, None]]]) -> List[str]:
        """
        Extracts member names from the configured range.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            List[str]: List of member names.
        """
        col_idx = self.config.col_member_idx
        return [
            row[col_idx]
            for row in sheet_data[self.config.row_members_start - 1 : self.config.row_members_end]
            if row[col_idx]
        ]

    def _create_backlog(
        self,
        sheet_data: List[List[Union[str, None]]],
        header_idxs: Dict[str, int],
    ) -> Dict[str, Task]:
        """
        Extracts tasks from the sheet data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Set[Task]: A set of unique Task objects.
        """

        tasks = {}
        for row in sheet_data[self.config.row_epics_start : self.config.row_epics_end]:
            code = row[header_idxs.get("code")]
            if code and code.lower() not in (task.lower() for task in self.config.epics_to_ignore):
                jira = row[header_idxs.get("jira")] if row[header_idxs.get("jira")] != "" else None
                description = (
                    row[header_idxs.get("subject")]
                    if row[header_idxs.get("subject")] != ""
                    else None
                )
                task_type = (
                    row[header_idxs.get("type")] if row[header_idxs.get("type")] != "" else None
                )
                if code:
                    tasks[code] = Task(
                        code=code, jira=jira, description=description, type=task_type
                    )

        return tasks

    def _find_last_day_column(self, sheet_data: List[List[Union[str, None]]]) -> int:
        """
        Finds the last valid column based on task cycle days.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            int: The index of the last valid column.
        """

        days_row = sheet_data[self.config.row_days]
        for idx, cell in enumerate(
            days_row[self.config.col_epics_assignment_start_idx :],
            start=self.config.col_epics_assignment_start_idx,
        ):
            if cell is None or cell == "":
                return idx
        return len(days_row)

    def _extract_task_duration(self, sheet_data: List[List[Union[str, None]]]) -> None:
        """
        Extracts the duration of each task, identifying the first and last day
        the task appears in the worksheet.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, Tuple[str, str]]: A dictionary with the task as the key
                                        and a tuple (start_date, end_date) as the value.
        """
        col_start_idx = self.config.col_epics_assignment_start_idx
        col_end_idx = self._find_last_day_column(sheet_data)

        epics_to_remove = {t.lower() for t in self.config.epics_to_ignore or []}

        for row in sheet_data[
            self.config.row_epics_assignment_start : self.config.row_epics_assignment_end
        ]:
            for col_idx in range(col_start_idx, col_end_idx):
                epic = row[col_idx]
                if epic and epic.lower() not in epics_to_remove:
                    new_date = sheet_data[self.config.row_days][col_idx]
                    task = self.backlog.get(epic)
                    if not task:
                        logger.debug(f"Task {epic} not found in the backlog.")
                        continue

                    task.execution_duration = (
                        task.execution_duration + 1 if task.execution_duration else 1
                    )

                    if task.start_date is None or new_date < task.start_date:
                        task.start_date = new_date
                    if task.end_date is None or new_date > task.end_date:
                        task.end_date = new_date
