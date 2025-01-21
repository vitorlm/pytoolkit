from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
import json
from pathlib import Path
from typing import Dict, List, Optional, Union

from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager
from ..core.config import Config
from utils.base_processor import BaseProcessor
from ..core.task import TaskSummary
from ..core.team_summary import TeamSummary
from ..core.cycle import Cycle

logger = LogManager.get_instance().get_logger("TeamTaskProcessor")


@dataclass
class TaskDetail:
    code: str = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    execution_duration: int = 0
    member_list: List[str] = field(default_factory=list)


class TeamTaskProcessor(BaseProcessor):
    """
    Processor for extracting task/epic allocation from Excel files.
    """

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsm", ".xlsx"])
        self.config = Config()
        self.team_summary = TeamSummary()

    def process_file(self, file_path: Union[str, Path]) -> TeamSummary:
        """
        Processes an Excel file to extract task allocations for team members.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.

        Returns:
            TeamSummary: Summary of the team's task data.

        Raises:
            ValueError: If the file contains invalid data.
        """
        file_path = Path(file_path)
        FileManager.validate_file(file_path, allowed_extensions=self.allowed_extensions)

        relevant_sheets = ExcelManager.filter_sheets_by_pattern(file_path, pattern=r"Q[1-4]-C[1-2]")
        logger.info(f"Processing {len(relevant_sheets)} relevant sheets from {file_path}")

        for sheet_name in relevant_sheets:
            sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name=sheet_name)
            logger.info(f"Processing sheet: {sheet_name}")
            self.process_sheet(sheet_name, sheet_data)

        output = json.dumps(
            self.team_summary,
            default=lambda o: (
                o.__dict__ if not isinstance(o, (date, datetime)) else o.strftime("%Y-%m-%d")
            ),
            ensure_ascii=False,
        )

        self.team_summary.summarize()

        return self.team_summary

    def process_sheet(self, cycle_name: str, cycle_data: List[List[Union[str, None]]]):
        """
        Processes a single cycle and updates the team summary.

        Args:
            cycle_name (str): The name of the cycle.
            cycle_data (List[List[Union[str, None]]]): The sheet data as rows of values.
        """
        header_idxs = self._extract_header(cycle_data)
        cycle = Cycle(cycle_name, self.config)

        self._find_cycle_dates(cycle, cycle_data)
        self._extract_members(cycle, cycle_data)

        cycle_duration = (cycle.end_date - cycle.start_date).days + 1
        weekdays = sum(
            1 for i in range(cycle_duration) if (cycle.start_date + timedelta(days=i)).weekday() < 5
        )
        cycle.effective_duration = weekdays * len(cycle.member_list)
        self._create_backlog(cycle, cycle_data, header_idxs)

        self._extract_task_durations(
            cycle,
            cycle_data,
            self.config.row_planned_epics_assignment_start,
            self.config.row_planned_epics_assignment_end,
            cycle.planned_tasks,
        )

        self._extract_task_durations(
            cycle,
            cycle_data,
            self.config.row_epics_assignment_start,
            self.config.row_epics_assignment_end,
            cycle.executed_tasks,
        )

        cycle.summarize_tasks()

        cycle.summarize()

        self.team_summary.add_cycle(cycle_name, cycle)

    def _extract_header(self, sheet_data: List[List[Union[str, None]]]) -> Dict[str, int]:
        """
        Extracts the header row from the sheet data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, int]: A dictionary mapping header items to their column indices.
        """
        header_map = {}
        for row_idx in range(self.config.row_header_start, self.config.row_header_end):
            for col_idx, cell_value in enumerate(sheet_data[row_idx]):
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

    def _extract_members(self, cycle: Cycle, sheet_data: List[List[Union[str, None]]]):
        """
        Extracts member names from the sheet data.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
        """
        col_idx = self.config.col_member_idx
        cycle.member_list = [
            row[col_idx]
            for row in sheet_data[self.config.row_members_start : self.config.row_members_end]
            if row[col_idx]
        ]

    def _create_backlog(
        self, cycle: Cycle, sheet_data: List[List[Union[str, None]]], header_idxs: Dict[str, int]
    ):
        """
        Creates a backlog of tasks from the sheet data.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            header_idxs (Dict[str, int]): Column indices for key headers.
        """
        for row in sheet_data[self.config.row_epics_start : self.config.row_epics_end]:
            code = row[header_idxs.get("code")]
            if code:
                jira = row[header_idxs.get("jira")]
                description = row[header_idxs.get("subject")]
                task_type = row[header_idxs.get("type")]

                if code not in cycle.backlog:
                    cycle.backlog[code] = TaskSummary(
                        code=code, jira=jira, description=description, type=task_type
                    )
                else:
                    logger.warning(f"Duplicate task code found: {code}")

    def _find_cycle_dates(self, cycle: Cycle, sheet_data: List[List[Union[str, None]]]):
        """
        Determines the start and end dates of a cycle from the sheet data.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
        """
        days_row = sheet_data[self.config.row_days]
        dates = [cell for cell in days_row if isinstance(cell, date)]
        if dates:
            cycle.start_date, cycle.end_date = min(dates), max(dates)

    def _extract_task_durations(
        self,
        cycle: Cycle,
        sheet_data: List[List[Union[str, None]]],
        row_start: int,
        row_end: int,
        task_map: Dict[str, TaskDetail],
    ):
        """
        Extracts task durations and updates the task map.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            row_start (int): Start row for task data.
            row_end (int): End row for task data.
            task_map (Dict[str, TaskDetail]): TaskDetail map to update.
        """
        col_start_idx = self.config.col_epics_assignment_start_idx
        col_end_idx = len(sheet_data[self.config.row_days])

        for row in sheet_data[row_start:row_end]:
            member = row[self.config.col_member_idx]
            for col_idx in range(col_start_idx, col_end_idx):
                task_code = row[col_idx]
                if task_code and task_code in cycle.backlog:
                    date_cell = sheet_data[self.config.row_days][col_idx]
                    if isinstance(date_cell, date):
                        task_detail = task_map.get(task_code, TaskDetail())
                        task_detail.code = task_code
                        task_detail.start_date = min(task_detail.start_date or date_cell, date_cell)
                        task_detail.end_date = max(task_detail.end_date or date_cell, date_cell)
                        task_detail.execution_duration += 1
                        if member and member not in task_detail.member_list:
                            task_detail.member_list.append(member)
                        task_map[task_code] = task_detail
