from typing import Dict, List, Union, Set, Optional, Literal
from pathlib import Path
from utils.excel_manager import ExcelManager
from utils.file_manager import FileManager
from .config import Config
from log_config import log_manager
from pydantic import BaseModel


class Task(BaseModel):
    """
    Model for validating task data.

    Attributes:
        code (str): Task code.
        jira (Optional[str]): JIRA issue key.
        description (Optional[str]): Task description.
        type (Literal["Eng", "Prod", "Day-by-Day", "Out", "Bug"]): Task type.
    """

    code: str
    jira: Optional[str]
    description: Optional[str]
    type: Literal["Eng", "Prod", "Day-by-Day", "Out", "Bug"]

    def __hash__(self):
        """
        Generates a hash value based on the task's code, jira, and description.

        Returns:
            int: Hash value.
        """
        return hash((self.code, self.jira, self.description, self.type))

    def __eq__(self, other):
        """
        Compares two Task objects for equality.

        Args:
            other (Task): Another Task object.

        Returns:
            bool: True if both objects are equal, False otherwise.
        """
        if not isinstance(other, Task):
            return False
        return (
            self.code == other.code
            and self.jira == other.jira
            and self.description == other.description
            and self.type == other.type
        )


class TaskProcessor:
    """
    Processor for extracting task/epic allocation from Excel files.
    """

    def __init__(self):
        self.task_map: Dict[str, List[str]] = {}
        self.logger = log_manager.get_logger(module_name=FileManager.get_module_name(__file__))
        log_manager.add_custom_handler(
            logger_name="openpyxl", replace_existing=True, handler_id="task_processor_httpx"
        )

    def process_folder(self, folder_path: Union[str, Path]) -> Dict[str, List[str]]:
        """
        Processes all Excel files in a folder to extract task allocations.

        Args:
            folder_path (Union[str, Path]): Path to the folder containing Excel files.

        Returns:
            Dict[str, List[str]]: Aggregated task map from all files.

        Raises:
            FileNotFoundError: If the specified folder does not exist.
            ValueError: If any file contains invalid data.
        """
        folder_path = Path(folder_path)
        self.logger.info(f"Loading Excel files from {folder_path}")

        # Validate folder path
        FileManager.validate_folder(folder_path)

        all_task_maps: Dict[str, List[str]] = {}
        for file_path in folder_path.glob("*.xls*"):
            try:
                self.logger.info(f"Processing file: {file_path}")
                file_task_map = self._process_file(file_path)
                self.logger.info(f"Processed {len(file_task_map)} tasks from {file_path}")
                all_task_maps.update(file_task_map)
            except Exception as e:
                self.logger.error(f"Error processing file {file_path}: {e}")

        return all_task_maps

    def _process_file(self, file_path: Union[str, Path]) -> Dict[str, List[str]]:
        """
        Processes an Excel file to extract task allocations for team members.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.

        Returns:
            Dict[str, List[str]]: Task map with members as keys and their tasks as values.

        Raises:
            ValueError: If the file contains invalid data.
        """
        file_path = Path(file_path)

        # Validate file extension
        FileManager.validate_file(file_path, allowed_extensions=[".xlsm", ".xlsx"])

        # Filter sheets to process based on naming convention
        relevant_sheets = ExcelManager.filter_sheets_by_pattern(file_path, pattern=r"Q[1-4]-C[1-2]")

        self.logger.info(f"Processing {len(relevant_sheets)} relevant sheets from {file_path}")
        for sheet_name in relevant_sheets:
            sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name=sheet_name)
            self.logger.info(f"Processing sheet: {sheet_name}")
            self._process_sheet(sheet_data)

        return self.task_map

    def _process_sheet(self, sheet_data: List[List[Union[str, None]]]):
        """
        Processes a single sheet to map tasks to team members.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data as rows of values.
        """
        header_idxs = self._extract_header(sheet_data)

        # Extract member names
        members = self._extract_members(sheet_data)

        tasks_backlog = self._extract_tasks(sheet_data, header_idxs)

        self.logger.info(f"Processing {len(members)} members and {len(tasks_backlog)} tasks")
        for row_idx, member in enumerate(members):
            member_row_idx = Config.ROW_MEMBERS_START - 1 + row_idx
            tasks = self._extract_tasks_per_member(sheet_data, member_row_idx, tasks_backlog)

            if member not in self.task_map:
                self.task_map[member] = set()

            # Add tasks to the set, avoiding duplicates
            self.task_map[member].update(tasks)

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
        for row_idx in range(Config.ROW_HEADER_START, Config.ROW_HEADER_END + 1):
            for col_idx, cell_value in enumerate(sheet_data[row_idx - 1]):
                if cell_value.lower() in ["code", "jira", "subject", "type"]:
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
        self.logger.debug(
            f"Extracting members from rows {Config.ROW_MEMBERS_START}-{Config.ROW_MEMBERS_END}"
        )
        col_idx = ord(Config.COL_MEMBERS.upper()) - ord("A")
        return [
            row[col_idx]
            for row in sheet_data[Config.ROW_MEMBERS_START - 1 : Config.ROW_MEMBERS_END]
            if row[col_idx]
        ]

    def _extract_tasks(
        self,
        sheet_data: List[List[Union[str, None]]],
        header_idxs: Dict[str, int],
    ) -> Set[Task]:
        """
        Extracts tasks from the sheet data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Set[Task]: A set of unique Task objects.
        """
        self.logger.debug(
            f"Extracting tasks from rows "
            f"{Config.ROW_TASKS_ASSIGNMENT_START}-{Config.ROW_TASKS_ASSIGNMENT_END}"
        )

        row_idx_tasks_start = Config.ROW_TASKS_ASSIGNMENT_START - 1
        row_idx_tasks_end = Config.ROW_TASKS_ASSIGNMENT_END

        # Transform the extracted data into Task objects
        tasks = set()
        for row in sheet_data[row_idx_tasks_start:row_idx_tasks_end]:
            code = row[header_idxs.get("code")]
            if code and code.lower() not in (task.lower() for task in Config.TASKS_TO_IGNORE):
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
                    tasks.add(Task(code=code, jira=jira, description=description, type=task_type))

        return tasks

    def _extract_tasks_per_member(
        self,
        sheet_data: List[List[Union[str, None]]],
        member_row_idx: int,
        tasks_backlog: Set[Task],
    ) -> Set[Task]:
        """
        Extracts Task objects from a specific row in the sheet, matching
        task codes with the list of tasks, and excludes tasks that should be ignored.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            member_row_idx (int): The row index for member's tasks.

        Returns:
            Set[Task]: A set of unique Task objects assigned to the member.
        """
        if not sheet_data or not isinstance(sheet_data, list):
            raise ValueError("sheet_data must be a non-empty list of lists.")

        # Validate task_row_idx
        if not (0 <= member_row_idx < len(sheet_data)):
            raise IndexError("task_row_idx is out of bounds.")

        # Retrieve the row for tasks
        task_row = sheet_data[member_row_idx]
        if not isinstance(task_row, list):
            raise ValueError(f"The row at index {member_row_idx} is not a valid list.")

        # Define column indices
        col_start_idx = ord(Config.COL_TASKS_ASSIGNMENT_START.upper()) - ord("A")
        col_end_idx = self._find_last_day_column(sheet_data)

        # Ensure column indices are valid
        if not (0 <= col_start_idx < len(task_row)):
            raise IndexError("Starting column index is out of bounds.")
        if not (col_start_idx < col_end_idx <= len(task_row)):
            raise IndexError("Ending column index is out of bounds or invalid.")

        # Extract task codes from the row
        task_codes = {cell for cell in task_row[col_start_idx:col_end_idx] if cell}

        # Remove tasks to ignore
        tasks_to_remove = {t.lower() for t in Config.TASKS_TO_IGNORE or []}
        filtered_task_codes = {code for code in task_codes if code.lower() not in tasks_to_remove}

        # Match task codes with tasks_backlog
        matched_tasks = {task for task in tasks_backlog if task.code in filtered_task_codes}

        return matched_tasks

    def _find_last_day_column(self, sheet_data: List[List[Union[str, None]]]) -> int:
        """
        Finds the last valid column based on task cycle days.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            int: The index of the last valid column.
        """
        col_idx_tasks_assignment_start = ord(Config.COL_TASKS_ASSIGNMENT_START.upper()) - ord("A")
        days_row = sheet_data[Config.ROW_DAYS - 1]
        for idx, cell in enumerate(
            days_row[col_idx_tasks_assignment_start:], start=col_idx_tasks_assignment_start
        ):
            if cell is None or cell == "":
                return idx
        return len(days_row)

    def _extract_range(
        self,
        sheet_data: List[List[Union[str, None]]],
        start_row: int,
        end_row: int,
        start_col: int,
        end_col: int,
    ) -> List[List[Union[str, None]]]:
        """
        Extracts a specific range of data from the sheet_data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The entire sheet data as a list of lists.
            start_row (int): The starting row index (0-based).
            end_row (int): The ending row index (exclusive, 0-based).
            start_col (int): The starting column index (0-based).
            end_col (int): The ending column index (exclusive, 0-based).

        Returns:
            List[List[Union[str, None]]]: The filtered range of rows and columns.
        """
        return [row[start_col:end_col] for row in sheet_data[start_row:end_row]]
