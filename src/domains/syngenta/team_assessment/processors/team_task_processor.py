from datetime import date
from pathlib import Path

import pandas as pd

from utils.base_processor import BaseProcessor
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager

from ..core.config import Config
from ..core.cycle import Cycle
from ..core.issue import Issue, IssueDetails
from ..core.team import Team
from ..services.jira_issue_fetcher import JiraIssueFetcher


class TeamTaskProcessor(BaseProcessor):
    """Processor for extracting task/epic allocation from Excel files."""

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsm", ".xlsx"])
        self._config = Config()
        self._team = None

    def process_file(
        self,
        file_path: str | Path,
        jira_project: str | None = None,
        team_name: str | None = None,
    ) -> Team:
        """Processes an Excel file to extract task allocations for team members.

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
        self.logger.info(f"Processing {len(relevant_sheets)} relevant sheets from {file_path}")

        self._team = Team(name=team_name, config=self._config)

        for sheet_name in relevant_sheets:
            sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name=sheet_name)
            self.logger.info(f"Processing sheet: {sheet_name}")
            self.process_sheet(sheet_name, sheet_data, jira_project, team_name)

        self._team.summarize()

        delattr(self._team, "_config")
        # output = json.dumps(
        #     self._team,
        #     default=lambda o: (
        #         o.__dict__ if not isinstance(o, (date, datetime)) else o.strftime("%Y-%m-%d")
        #     ),
        #     ensure_ascii=False,
        # )

        return self._team

    def process_sheet(
        self,
        cycle_name: str,
        cycle_data: list[list[str | None]],
        jira_project: str | None = None,
        team_name: str | None = None,
    ):
        """Processes a single cycle and updates the team summary.

        Args:
            cycle_name (str): The name of the cycle.
            cycle_data (List[List[Union[str, None]]]): The sheet data as rows of values.
        """
        header_idxs = self._extract_header(cycle_data)
        cycle = Cycle(config=self._config, name=cycle_name)

        self._extract_members(cycle, cycle_data)

        self._find_cycle_dates(cycle, cycle_data)

        if jira_project and team_name:
            self._load_bugs(cycle, jira_project, team_name)

        self._create_backlog(cycle, cycle_data, header_idxs)

        self._extract_issue_durations(
            cycle,
            cycle_data,
            self._config.row_planned_epics_assignment_start,
            self._config.row_planned_epics_assignment_end,
            "planned",
        )

        self._extract_issue_durations(
            cycle,
            cycle_data,
            self._config.row_epics_assignment_start,
            self._config.row_epics_assignment_end,
            "executed",
        )

        cycle.closed_epics = self._load_planned_epics_closed_within_date_range(
            cycle,
            [
                epic.jira
                for epic in cycle.backlog.values()
                if epic.jira and epic.code not in self._config.issue_helper_codes
            ],
        )

        cycle.spillover_epics = self._load_unplanned_epics_closed_within_date_range(
            cycle, jira_project, team_name, cycle.start_date, cycle.end_date
        )

        cycle.summarize_issues()

        cycle.summarize()

        delattr(cycle, "_config")
        self._team.add_cycle(cycle_name, cycle)

    def _extract_header(self, sheet_data: list[list[str | None]]) -> dict[str, int]:
        """Extracts the header row from the sheet data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, int]: A dictionary mapping header items to their column indices.
        """
        header_map = {}
        for row_idx in range(self._config.row_header_start, self._config.row_header_end):
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

    def _extract_members(self, cycle: Cycle, sheet_data: list[list[str | None]]):
        """Extracts member names from the sheet data.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
        """
        col_idx = self._config.col_member_idx
        cycle.member_list = [
            row[col_idx]
            for row in sheet_data[self._config.row_members_start : self._config.row_members_end]
            if row[col_idx]
        ]

    def _create_backlog(
        self,
        cycle: Cycle,
        sheet_data: list[list[str | None]],
        header_idxs: dict[str, int],
    ):
        """Creates a backlog of tasks from the sheet data.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            header_idxs (Dict[str, int]): Column indices for key headers.
        """
        for row in sheet_data[self._config.row_epics_start : self._config.row_epics_end]:
            code = row[header_idxs.get("code")]
            if code:
                code = code.lower()
                jira = row[header_idxs.get("jira")]
                description = row[header_idxs.get("subject")]
                task_type = row[header_idxs.get("type")]

                if code not in cycle.backlog:
                    issue = Issue(code=code, jira=jira, description=description, type=task_type)
                    cycle.backlog[code] = issue
                else:
                    self.logger.warning(f"Duplicate task code found: {code}")

    def _find_cycle_dates(self, cycle: Cycle, sheet_data: list[list[str | None]]):
        """Determines the start and end dates of a cycle from the sheet data.

        Args:
            cycle (Cycle): The cycle object to update.
            sheet_data (List[List[Union[str, None]]]): The sheet data.
        """
        days_row = sheet_data[self._config.row_days]
        dates = [cell for cell in days_row if isinstance(cell, date)]
        if dates:
            cycle.start_date, cycle.end_date = (
                pd.Timestamp(min(dates)),
                pd.Timestamp(max(dates)),
            )

    def _extract_issue_durations(
        self,
        cycle: Cycle,
        sheet_data: list[list[str | None]],
        row_start: int,
        row_end: int,
        type: str,
    ):
        col_assignment_start = self._config.col_epics_assignment_start_idx
        col_assignment_end = len(sheet_data[self._config.row_days])

        for row in sheet_data[row_start:row_end]:
            member = row[self._config.col_member_idx]
            for col_idx in range(col_assignment_start, col_assignment_end):
                issue_code = row[col_idx].lower() if row[col_idx] else None
                if issue_code and issue_code in cycle.backlog:
                    date_cell = sheet_data[self._config.row_days][col_idx]
                    if isinstance(date_cell, date):
                        self._update_issue_details(cycle, issue_code, date_cell, member, type)

    def _update_issue_details(self, cycle: Cycle, issue_code: str, date_cell: date, member: str, type: str):
        issue = cycle.backlog[issue_code]
        try:
            if type == "planned":
                issue_details = issue.planned = issue.planned or IssueDetails()
                if issue_code not in cycle.planned_issues:
                    cycle.planned_issues.append(issue_code)
            else:
                issue_details = issue.executed = issue.executed or IssueDetails()
                if issue_code not in cycle.executed_issues:
                    cycle.executed_issues.append(issue_code)
        except KeyError:
            print(f"Issue code '{issue_code}' not found in backlog.")
            return

        issue_details.start_date = min(issue_details.start_date or date_cell, date_cell)
        issue_details.end_date = max(issue_details.end_date or date_cell, date_cell)
        issue_details.issue_total_days += 1
        if member and member not in issue_details.member_list:
            issue_details.member_list.append(member)

    def _load_bugs(self, cycle: Cycle, jira_project: str, team_name: str):
        """Fetches bug issues from Jira for a given cycle.

        Args:
            cycle (Cycle): The cycle object to update.
        """
        jira_issue = JiraIssueFetcher()
        cycle.bugs = jira_issue.get_bugs_created_within_dates(jira_project, team_name, cycle.start_date, cycle.end_date)

    def _load_planned_epics_closed_within_date_range(self, cycle: Cycle, epic_keys: list[str]) -> list[dict]:
        """Fetches epic issues from Jira and checks if the closed date is within the cycle's date range

        Args:
            cycle (Cycle): The cycle object containing start and end dates.
            epic_keys (List[str]): The list of epic keys to fetch.

        Returns:
            List[Dict]: A list of epic issues within the date range.
        """
        jira_issue = JiraIssueFetcher()
        epics = jira_issue.get_epics_by_keys(epic_keys)
        filtered_epics = [epic for epic in epics if cycle.start_date <= epic.closed_date <= cycle.end_date]
        return filtered_epics

    def _load_unplanned_epics_closed_within_date_range(
        self,
        cycle: Cycle,
        project_name: str,
        team_name: str,
        start_date: date,
        end_date: date,
    ) -> list[dict]:
        """Fetches epic issues from Jira and checks if the closed date is within the cycle's date range

        Args:
            cycle (Cycle): The cycle object containing start and end dates.
            epic_keys (List[str]): The list of epic keys to fetch.

        Returns:
            List[Dict]: A list of epic issues within the date range.
        """
        jira_issue = JiraIssueFetcher()
        epics = jira_issue.get_epics_closed_during_period(project_name, team_name, start_date, end_date)
        filtered_epics = [
            epic for epic in epics if epic.key not in [backlog_epic.jira for backlog_epic in cycle.backlog.values()]
        ]
        return filtered_epics
