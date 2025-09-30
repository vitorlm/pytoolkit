from pathlib import Path
from typing import Dict, List, Union, Set
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager
from ..core.config import Config
from utils.base_processor import BaseProcessor
from ..core.issue import Issue


class MembersTaskProcessor(BaseProcessor):
    """
    Processor for extracting task/epic allocation from Excel files.
    """

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsm", ".xlsx"])
        self._config = Config()
        self.task_map: Dict[str, Set[Issue]] = {}

    def process_file(self, file_path: Union[str, Path]) -> Dict[str, Set[Issue]]:
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
        relevant_sheets = ExcelManager.filter_sheets_by_pattern(
            file_path, pattern=r"Q[1-4]-C[1-2]"
        )

        self.logger.info(
            f"Processing {len(relevant_sheets)} relevant sheets from {file_path}"
        )
        for sheet_name in relevant_sheets:
            sheet_data = ExcelManager.read_excel_as_list(
                file_path, sheet_name=sheet_name
            )
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

        # Extract member names
        members = self._extract_members(sheet_data)

        tasks_backlog = self._extract_tasks(sheet_data, header_idxs)

        self.logger.info(
            f"Processing {len(members)} members and {len(tasks_backlog)} tasks"
        )
        for row_idx, member in enumerate(members):
            member_row_idx = self._config.row_members_start + row_idx
            tasks = self._extract_tasks_per_member(
                sheet_data, member_row_idx, tasks_backlog
            )

            if member not in self.task_map:
                self.task_map[member] = []

            # Add tasks to the set, avoiding duplicates
            self.task_map[member].extend(
                task for task in tasks if task not in self.task_map[member]
            )

    def _extract_header(
        self, sheet_data: List[List[Union[str, None]]]
    ) -> Dict[str, int]:
        """
        Extracts the header row from the sheet data and returns a dictionary
        mapping header items to their column indices.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, int]: A dictionary with header items as keys and column indices as values.
        """
        header_map = {}
        for row_idx in range(self._config.row_header_start, self._config.row_epics_end):
            for col_idx, cell_value in enumerate(sheet_data[row_idx - 1]):
                # Ensure cell_value is a string before calling .lower()
                if isinstance(cell_value, str) and cell_value.lower() in [
                    "code",
                    "jira",
                    "subject",
                    "type",
                ]:
                    header_map[cell_value.lower()] = col_idx
                    if len(header_map) == 4:
                        self.logger.info(f"Header extracted: {header_map}")
                        return header_map
        self.logger.warning(
            "Header extraction incomplete, some columns may be missing."
        )
        return header_map

    def _extract_members(self, sheet_data: List[List[Union[str, None]]]) -> List[str]:
        """
        Extracts member names from the configured range.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            List[str]: List of member names.
        """
        col_idx = self._config.col_member_idx
        members = [
            row[col_idx]
            for row in sheet_data[
                self._config.row_members_start : self._config.row_members_end
            ]
            if row[col_idx]
        ]
        self.logger.info(f"Extracted {len(members)} members")
        return members

    def _extract_tasks(
        self,
        sheet_data: List[List[Union[str, None]]],
        header_idxs: Dict[str, int],
    ) -> List[Issue]:
        """
        Extracts tasks from the sheet data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Set[Task]: A set of unique Task objects.
        """
        tasks = []
        for row in sheet_data[
            self._config.row_epics_start : self._config.row_epics_end
        ]:
            code = row[header_idxs.get("code")]
            if code and code not in (
                task.lower() for task in self._config._issue_helper_codes
            ):
                code = code.lower()
                jira = row[header_idxs.get("jira")]
                description = row[header_idxs.get("subject")]
                task_type = row[header_idxs.get("type")]
                if code:
                    tasks.append(
                        Issue(
                            code=code,
                            jira=jira,
                            description=description,
                            type=task_type,
                        )
                    )
        self.logger.info(f"Extracted {len(tasks)} tasks")
        return tasks

    def _extract_tasks_per_member(
        self,
        sheet_data: List[List[Union[str, None]]],
        member_row_idx: int,
        tasks_backlog: Set[Issue],
    ) -> List[Issue]:
        """
        Extracts Task objects from a specific row in the sheet, matching
        task codes with the list of tasks, and excludes tasks that should be ignored.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            member_row_idx (int): The row index for member's tasks.

        Returns:
            Set[Task]: A set of unique Task objects assigned to the member.
        """
        task_row = sheet_data[member_row_idx]
        col_start_idx = self._config.col_epics_assignment_start_idx
        col_end_idx = self._find_last_day_column(sheet_data)

        task_codes = {cell for cell in task_row[col_start_idx:col_end_idx] if cell}

        tasks_to_remove = {t.lower() for t in self._config._issue_helper_codes or []}
        filtered_task_codes = {
            code.lower()
            for code in task_codes
            if isinstance(code, str) and code.lower() not in tasks_to_remove
        }

        matched_tasks = [
            task for task in tasks_backlog if task.code in filtered_task_codes
        ]

        self.logger.info(
            f"Extracted {len(matched_tasks)} tasks for member at row {member_row_idx}"
        )
        return matched_tasks

    def _find_last_day_column(self, sheet_data: List[List[Union[str, None]]]) -> int:
        """
        Finds the last valid column based on task cycle days.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            int: The index of the last valid column.
        """
        col_idx_tasks_assignment_start = self._config.col_epics_assignment_start_idx
        days_row = sheet_data[self._config.row_days]
        for idx, cell in enumerate(
            days_row[col_idx_tasks_assignment_start:],
            start=col_idx_tasks_assignment_start,
        ):
            if cell is None or cell == "":
                self.logger.info(f"Last valid column found at index {idx}")
                return idx
        self.logger.info("All columns are valid for task assignment")
        return len(days_row)
