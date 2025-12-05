from pathlib import Path

from domains.syngenta.team_assessment.core.config import Config
from domains.syngenta.team_assessment.core.issue import Issue
from utils.base_processor import BaseProcessor
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager
from utils.string_utils import StringUtils


class MembersTaskProcessor(BaseProcessor):
    """Processor for extracting task/epic allocation from Excel files."""

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsm", ".xlsx"])
        self._config = Config()
        self.task_map: dict[str, list[Issue]] = {}

    def process_file(self, file_path: str | Path, **kwargs) -> dict[str, list[Issue]]:
        """Processes an Excel file to extract task allocations for team members.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.
            **kwargs: Additional parameters (for BaseProcessor compatibility).

        Returns:
            Dict[str, List[Issue]]: Task map with members as keys and their tasks as values.

        Raises:
            ValueError: If the file contains invalid data.
        """
        file_path_obj = Path(file_path)
        file_path_str = str(file_path_obj)

        # Validate file extension
        FileManager.validate_file(file_path_str, allowed_extensions=self.allowed_extensions)

        # Filter sheets to process based on naming convention
        relevant_sheets = ExcelManager.filter_sheets_by_pattern(file_path_str, pattern=r"Q[1-4]-C[1-2]")

        self.logger.info(f"Processing {len(relevant_sheets)} relevant sheets from {file_path}")
        for sheet_name in relevant_sheets:
            sheet_data = ExcelManager.read_excel_as_list(file_path_str, sheet_name=sheet_name)
            self.logger.info(f"Processing sheet: {sheet_name}")
            self.process_sheet(sheet_data, sheet_name=sheet_name)

        return self.task_map

    def process_sheet(self, sheet_data: list[list[str | None]], sheet_name: str | None = None, **kwargs):
        """Processes a single sheet to map tasks to team members.

        Flow:
        1. Extract members from column C (rows 4-14) - master list
        2. Extract tasks/epics from backlog (rows 6-33)
        3. For each member in the allocation matrix (column J, rows 39-47):
           - Find matching member from master list
           - Extract task codes from columns K-AS for that row
           - Match codes with tasks from backlog

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data as rows of values.
            sheet_name (str | None): Name of the sheet being processed (e.g., Q1-C1, Q2-C2).
            **kwargs: Additional keyword arguments (for BaseProcessor compatibility).
        """
        header_idxs = self._extract_header(sheet_data)

        # Step 1: Extract list of members (for reference/validation)
        member_reference_list = self._extract_members(sheet_data)
        self.logger.info(f"Master member list has {len(member_reference_list)} members")

        # Step 2: Extract tasks/epics from backlog
        tasks_backlog = self._extract_tasks(sheet_data, header_idxs)
        self.logger.info(f"Extracted {len(tasks_backlog)} tasks from backlog")

        # Step 3: Process allocation matrix (column J has members, K-AS have task codes)
        # Only members who appear in column J (result of FILTER formula) will be processed
        col_members_allocation = self._config.col_allocation_members_idx

        for row_idx in range(self._config.row_epics_assignment_start, self._config.row_epics_assignment_end):
            # Get member name from column J (allocation matrix)
            member_name_raw = sheet_data[row_idx][col_members_allocation]

            if not member_name_raw or not isinstance(member_name_raw, str):
                continue

            member_name = StringUtils.remove_accents(member_name_raw.strip())

            # Optional: Log member status from list
            if member_name in member_reference_list:
                member_info = member_reference_list[member_name]
                self.logger.debug(
                    f"Processing {member_name} "
                    f"(status: {member_info.get('status')}, discovery: {member_info.get('discovery')})"
                )

            # Extract tasks for this member
            tasks = self._extract_tasks_per_member(sheet_data, row_idx, tasks_backlog, sheet_name)

            # Calculate total days for this member in this sheet
            total_days_sheet = sum(
                getattr(task.executed, "issue_total_days", 0)
                for task in tasks
                if hasattr(task, "executed") and task.executed
            )
            self.logger.info(f"Member {member_name} in {sheet_name}: {len(tasks)} tasks, {total_days_sheet} total days")

            if member_name not in self.task_map:
                self.task_map[member_name] = []

            # Add all tasks from this sheet (each sheet/cycle is independent)
            # Don't deduplicate because the same task code can appear in multiple cycles
            self.task_map[member_name].extend(tasks)

        self.logger.info(f"Task map built with {len(self.task_map)} members: {list(self.task_map.keys())}")

    def _extract_header(self, sheet_data: list[list[str | None]]) -> dict[str, int]:
        """Extracts the header row from the sheet data.

        Returns a dictionary mapping header items to their column indices.

        Expected headers:
        - priority: R# (rank/priority)
        - dev_type: Dev Type (BE/FE)
        - code: Code (task code)
        - jira: JIRA (JIRA ticket)
        - subject: Subject (description)
        - type: Type (Eng/Prod/Day-by-Day/Out/Bug)

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, int]: A dictionary with header items as keys and column indices as values.
        """
        # Use fixed column positions from config
        header_map = {
            "priority": self._config.col_priority_idx,
            "dev_type": self._config.col_dev_type_idx,
            "code": self._config.col_code_idx,
            "jira": self._config.col_jira_idx,
            "subject": self._config.col_subject_idx,
            "type": self._config.col_type_idx,
        }
        self.logger.info(f"Header map: {header_map}")
        return header_map

    def _extract_members(self, sheet_data: list[list[str | None]]) -> dict[str, dict[str, str]]:
        """Extracts member information from the master list (rows 4-14).

        Reads:
        - Column B: Discovery participation (Y/N)
        - Column C: Member name
        - Column D: Status (Active, On loan, Seconded, Out)

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.

        Returns:
            Dict[str, Dict]: Member info keyed by normalized name.
                            {name: {discovery: Y/N, status: Active/On loan/...}}
        """
        members_info = {}
        col_discovery = 1  # Column B (index 1)
        col_name = self._config.col_member_idx  # Column C (index 2)
        col_status = 3  # Column D (index 3)

        for row in sheet_data[self._config.row_members_start : self._config.row_members_end]:
            name_raw = row[col_name] if col_name < len(row) else None
            if not name_raw:
                continue

            name = StringUtils.remove_accents(name_raw.strip())
            discovery = row[col_discovery] if col_discovery < len(row) else None
            status = row[col_status] if col_status < len(row) else None

            members_info[name] = {"discovery": discovery, "status": status, "raw_name": name_raw}

        self.logger.info(f"Extracted {len(members_info)} members from master list")
        return members_info

    def _extract_tasks(
        self,
        sheet_data: list[list[str | None]],
        header_idxs: dict[str, int],
    ) -> list[Issue]:
        """Extracts tasks from the sheet data (rows 6-33).

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            header_idxs (Dict[str, int]): Column indices for each header field.

        Returns:
            List[Issue]: List of unique Issue objects from backlog.
        """
        tasks = []
        # Helper codes are valid activities (Bug, Spillover, Out, etc.)
        # They should be included in the backlog for matching

        for row in sheet_data[self._config.row_epics_start : self._config.row_epics_end]:
            code = row[header_idxs["code"]]

            if not code:
                continue

            code_lower = code.lower() if isinstance(code, str) else str(code).lower()

            priority = row[header_idxs["priority"]]
            dev_type = row[header_idxs["dev_type"]]
            jira = row[header_idxs["jira"]]
            description = row[header_idxs["subject"]]
            type_raw = row[header_idxs["type"]]
            task_type = str(type_raw) if type_raw else "default_type"

            # Determine activity category for this task code
            category = self._config.get_category_for_code(code)

            tasks.append(
                Issue(
                    code=code_lower,
                    type=task_type,
                    priority=priority,
                    dev_type=dev_type,
                    jira=jira,
                    description=description,
                    closed=False,
                    planned=None,
                    executed=None,
                    summary=None,
                    category=category,
                )
            )

        # Log category distribution for debugging
        category_counts = {}
        for task in tasks:
            category_counts[task.category] = category_counts.get(task.category, 0) + 1

        self.logger.info(f"Extracted {len(tasks)} valid tasks from backlog")
        self.logger.info(f"Category distribution: {category_counts}")
        return tasks

    def _extract_tasks_per_member(
        self,
        sheet_data: list[list[str | None]],
        member_row_idx: int,
        tasks_backlog: list[Issue],
        sheet_name: str | None = None,
    ) -> list[Issue]:
        """Extracts Task objects from a specific row in the sheet with allocated days count.

        Counts how many days each task appears in the member's allocation row and
        stores this information in the task's executed field for productivity metrics.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            member_row_idx (int): The row index for member's tasks.
            tasks_backlog (list[Issue]): List of all available tasks from backlog.
            sheet_name (str | None): Name of the sheet being processed (e.g., Q1-C1, Q2-C2).

        Returns:
            list[Issue]: A list of Task objects with allocated days information.
        """
        from .team_task_processor import IssueDetails

        task_row = sheet_data[member_row_idx]
        col_start_idx = self._config.col_epics_assignment_start_idx
        col_end_idx = self._find_last_day_column(sheet_data, sheet_name)

        self.logger.debug(
            f"Extracting tasks for row {member_row_idx}: col_start={col_start_idx}, col_end={col_end_idx}"
        )

        # Count days per task code and empty cells (NotPlanned)
        task_code_days = {}
        empty_cells_count = 0
        for col_idx in range(col_start_idx, col_end_idx):
            cell = task_row[col_idx]
            if cell and isinstance(cell, str) and cell.strip():
                code_lower = cell.lower()
                task_code_days[code_lower] = task_code_days.get(code_lower, 0) + 1
            else:
                # Count empty cells (None, empty string, or whitespace only)
                empty_cells_count += 1

        self.logger.debug(f"Task codes with day counts: {task_code_days}")
        self.logger.debug(f"Empty cells (NotPlanned days): {empty_cells_count}")

        # Match task codes with backlog and enrich with allocated days
        matched_tasks = []
        matched_codes = set()  # Track which codes were matched

        for task in tasks_backlog:
            code_lower = task.code.lower()
            if code_lower in task_code_days:
                # Create a copy of the task with execution data
                allocated_days = task_code_days[code_lower]

                # Create IssueDetails with allocated days information
                executed_details = IssueDetails(
                    issue_total_days=allocated_days,
                    start_date=None,
                    end_date=None,
                    days_of_week=[],
                )

                # Create enriched task with execution data
                enriched_task = Issue(
                    code=task.code,
                    type=task.type,
                    priority=task.priority,
                    dev_type=task.dev_type,
                    jira=task.jira,
                    description=task.description,
                    closed=task.closed,
                    planned=task.planned,
                    executed=executed_details,
                    summary=task.summary,
                    category=task.category,
                )

                matched_tasks.append(enriched_task)
                matched_codes.add(code_lower)

        # Find unmapped codes (codes in allocation that weren't in backlog)
        unmapped_codes = set(task_code_days.keys()) - matched_codes

        # Create UNPLANNED tasks for unmapped codes
        for unmapped_code in unmapped_codes:
            allocated_days = task_code_days[unmapped_code]
            self.logger.warning(
                f"Code '{unmapped_code}' found in allocation but not in backlog. "
                f"Creating UNPLANNED task with {allocated_days} days."
            )

            unplanned_task = Issue(
                code=unmapped_code,
                type="Day-by-Day",  # Unmapped codes are treated as day-to-day work
                priority="N/A",
                dev_type="N/A",
                jira=None,
                description=f"Unmapped code from allocation matrix: {unmapped_code}",
                closed=False,
                planned=None,
                executed=IssueDetails(
                    issue_total_days=allocated_days,
                    start_date=None,
                    end_date=None,
                    days_of_week=[],
                ),
                summary=None,
                category="UNPLANNED",
            )
            matched_tasks.append(unplanned_task)

        # Add IDLE task for empty cells (unallocated time - doesn't count toward total days in team)
        if empty_cells_count > 0:
            idle_task = Issue(
                code="idle",
                type="Idle",
                priority="N/A",
                dev_type="N/A",
                jira=None,
                description="Unallocated time (empty cells in planning matrix)",
                closed=False,
                planned=None,
                executed=IssueDetails(
                    issue_total_days=empty_cells_count,
                    start_date=None,
                    end_date=None,
                    days_of_week=[],
                ),
                summary=None,
                category="IDLE",
            )
            matched_tasks.append(idle_task)
            self.logger.debug(f"Added IDLE task with {empty_cells_count} days (empty cells)")

        self.logger.info(
            f"Extracted {len(matched_tasks)} tasks for member at row {member_row_idx} (Excel row {member_row_idx + 1})"
        )

        # Log summary of allocated days
        total_days = sum(task.executed.issue_total_days for task in matched_tasks if task.executed)
        self.logger.debug(f"Total allocated days: {total_days}")

        return matched_tasks

    def _find_last_day_column(self, sheet_data: list[list[str | None]], sheet_name: str | None = None) -> int:
        """Finds the last valid column based on task cycle days.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data.
            sheet_name (str | None): The name of the sheet (e.g., Q1-C1, Q2-C2) to determine cycle days.

        Returns:
            int: The index of the last valid column.
        """
        col_idx_tasks_assignment_start = self._config.col_epics_assignment_start_idx

        # Determine cycle days based on sheet name
        cycle_days = None
        if sheet_name:
            if sheet_name.endswith("-C1"):
                cycle_days = self._config.cycle_c1_days
                self.logger.debug(f"Using C1 cycle configuration: {cycle_days} days")
            elif sheet_name.endswith("-C2"):
                cycle_days = self._config.cycle_c2_days
                self.logger.debug(f"Using C2 cycle configuration: {cycle_days} days")

        # If we have cycle days configuration, use it directly
        if cycle_days:
            last_col_idx = col_idx_tasks_assignment_start + cycle_days
            self.logger.info(f"Using configured cycle days: {cycle_days}, last column index: {last_col_idx}")
            return last_col_idx

        # Fallback: scan days_row for empty cells or formulas
        days_row = sheet_data[self._config.row_days]
        for idx, cell in enumerate(
            days_row[col_idx_tasks_assignment_start:],
            start=col_idx_tasks_assignment_start,
        ):
            # Check for empty cells or Excel formulas (strings starting with =)
            if cell is None or cell == "" or (isinstance(cell, str) and cell.startswith("=")):
                self.logger.info(f"Last valid column found at index {idx}")
                return idx
        self.logger.info("All columns are valid for task assignment")
        return len(days_row)
