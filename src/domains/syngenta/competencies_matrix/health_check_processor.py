from typing import Dict, List, Union, Optional
from pathlib import Path
from log_config import log_manager
from utils.excel_manager import ExcelManager
from utils.file_manager import FileManager

from datetime import datetime
import statistics


class FeedbackAssessment:
    """
    Model for validating health check data.

    Attributes:
        date (date): Date of the feedback.
        feedback_by (str): Name of the person providing the feedback.
        effort (int): Effort score (1-5).
        effort_comment (Optional[str]): Comment about the effort score.
        impact (int): Impact score (1-5).
        impact_comment (Optional[str]): Comment about the impact score.
        morale (int): Morale score (1-5).
        morale_comment (Optional[str]): Comment about the morale score.
        retention (int): Retention risk score (1-5).
        retention_comment (Optional[str]): Comment about the retention risk score.
    """

    health_check_date: str
    feedback_by: str
    effort: Optional[int]
    effort_comment: Optional[str]
    impact: Optional[int]
    impact_comment: Optional[str]
    morale: Optional[int]
    morale_comment: Optional[str]
    retention: Optional[int]
    retention_comment: Optional[str]

    def __init__(
        self,
        health_check_date: str,
        feedback_by: str,
        effort: Optional[int],
        effort_comment: Optional[str],
        impact: Optional[int],
        impact_comment: Optional[str],
        morale: Optional[int],
        morale_comment: Optional[str],
        retention: Optional[int],
        retention_comment: Optional[str],
    ):
        self.health_check_date = health_check_date
        self.feedback_by = feedback_by
        self.effort = effort
        self.effort_comment = effort_comment
        self.impact = impact
        self.impact_comment = impact_comment
        self.morale = morale
        self.morale_comment = morale_comment
        self.retention = retention
        self.retention_comment = retention_comment


class CorrelationSummary:
    def __init__(
        self,
        effort_vs_impact: Optional[float] = None,
        effort_vs_morale: Optional[float] = None,
        effort_vs_retention: Optional[float] = None,
        impact_vs_morale: Optional[float] = None,
        impact_vs_retention: Optional[float] = None,
        morale_vs_retention: Optional[float] = None,
    ):
        self.effort_vs_impact = effort_vs_impact
        self.effort_vs_morale = effort_vs_morale
        self.effort_vs_retention = effort_vs_retention
        self.impact_vs_morale = impact_vs_morale
        self.impact_vs_retention = impact_vs_retention
        self.morale_vs_retention = morale_vs_retention


class HealthCheckStatistics:
    def __init__(
        self,
        sample_size: int,
        means: Dict[str, Optional[float]],
        std_devs: Dict[str, Optional[float]],
        correlations: CorrelationSummary,
    ):
        self.sample_size = sample_size
        self.means = {
            "effort": means.get("effort"),
            "impact": means.get("impact"),
            "morale": means.get("morale"),
            "retention": means.get("retention"),
        }
        self.std_devs = {
            "effort": std_devs.get("effort"),
            "impact": std_devs.get("impact"),
            "morale": std_devs.get("morale"),
            "retention": std_devs.get("retention"),
        }
        self.correlations = correlations


class MemberHealthCheck:

    feedback_data: Optional[List[FeedbackAssessment]] = None
    statistics: Optional[HealthCheckStatistics] = None

    def __init__(
        self,
        health_check: Optional[List[FeedbackAssessment]] = [],
        statistics: Optional[HealthCheckStatistics] = [],
    ):
        self.feedback_data = health_check or []
        self.statistics = statistics or []


class HealthCheckProcessor:
    """
    Processor for extracting Health Check data from Excel files.
    """

    def __init__(self):
        self.health_check_data: Dict[str, List[Dict[str, Union[str, int]]]] = {}
        self.logger = log_manager.get_logger(module_name=FileManager.get_module_name(__file__))
        log_manager.add_custom_handler(
            logger_name="openpyxl", replace_existing=True, handler_id="health_check_processor"
        )

    def process_folder(self, folder_path: Union[str, Path]) -> Dict[str, MemberHealthCheck]:
        """
        Processes all Excel files in a folder to extract Health Check data for team members.

        Args:
            folder_path (Union[str, Path]): Path to the folder containing Excel files.

        Returns:
            Dict[str, List[Dict[str, Union[str, int]]]]: Health Check data grouped by member.

        Raises:
            FileNotFoundError: If the folder does not exist.
            ValueError: If the folder does not contain any valid Excel files.
        """
        folder_path = Path(folder_path)
        self.logger.info(f"Processing folder: {folder_path}")

        # Validate folder path
        FileManager.validate_folder(folder_path)

        health_check_data: Dict[str, MemberHealthCheck] = {}
        for file_path in folder_path.glob("*.xlsx"):
            try:
                self.logger.info(f"Processing file: {file_path}")
                health_check = self._process_file(file_path)
                health_check_data.update(health_check)
            except ValueError as e:
                self.logger.error(f"Error processing file '{file_path}': {e}", exc_info=True)
                raise

        return health_check_data

    def _process_file(
        self,
        file_path: Union[str, Path],
    ) -> Dict[str, MemberHealthCheck]:
        """
        Processes a single file to extract Health Check data and calculates statistics.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.

        Returns:
            Dict[str, HealthCheckData]: A dictionary containing raw HealthCheck data
            and statistics for each relevant sheet.
        """
        file_path = Path(file_path)

        FileManager.validate_file(file_path, allowed_extensions=[".xlsx"])

        relevant_sheets = ExcelManager.filter_sheets_by_pattern(
            file_path, pattern="^(?!Example$|Data$)"
        )

        members_health_check: Dict[str, MemberHealthCheck] = {}

        for sheet_name in relevant_sheets:
            member_name = sheet_name.split(" ")[0]
            sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name=sheet_name)
            self.logger.info(f"Processing health check sheet: {member_name}")
            feedback_assessments = self._process_sheet(sheet_data)

            if len(feedback_assessments) > 3:
                # Calculate statistics for the current sheet
                statistics = self._calculate_statistics(feedback_assessments)
            else:
                statistics = []

            members_health_check[member_name] = MemberHealthCheck(
                health_check=feedback_assessments, statistics=statistics
            )

        return members_health_check

    def _process_sheet(self, sheet_data: List[List[Union[str, None]]]) -> List[FeedbackAssessment]:
        """
        Processes a single sheet to extract Health Check data.

        Args:
            sheet_data (List[List[Union[str, None]]]): The sheet data as rows of values.

        Returns:
            List[HealthCheck]: List of Health Check objects for the member.
        """
        records = []
        header = sheet_data[0]  # Assuming the first row is the header
        header_map = {col_name.lower(): idx for idx, col_name in enumerate(header) if col_name}

        def parse_indicator(indicator) -> Optional[int]:
            return indicator if isinstance(indicator, int) else None

        for row in sheet_data[1:]:  # Skip the header row
            if all(cell is None for cell in row):  # Skip empty rows
                continue

            # Create a HealthCheck object using the mapped indices
            record = FeedbackAssessment(
                health_check_date=(
                    datetime.strptime(row[header_map.get("date")], "%Y-%m-%d").strftime("%d/%m/%Y")
                    if not isinstance(row[header_map.get("date")], datetime)
                    else row[header_map.get("date")].strftime("%d/%m/%Y")
                ),
                feedback_by=row[header_map.get("feedback by")],
                effort=parse_indicator(row[header_map.get("effort")]),
                effort_comment=row[header_map.get("effort comment")],
                impact=parse_indicator(row[header_map.get("impact")]),
                impact_comment=row[header_map.get("impact comment")],
                morale=parse_indicator(row[header_map.get("morale")]),
                morale_comment=row[header_map.get("morale comment")],
                retention=parse_indicator(row[header_map.get("retention")]),
                retention_comment=row[header_map.get("retention comment")],
            )
            records.append(record)
        return records

    def _calculate_statistics(
        self, health_checks: List[FeedbackAssessment]
    ) -> HealthCheckStatistics:
        """
        Calculates the mean, standard deviation, and simple correlations for Health Check data.

        Args:
            health_checks (List[FeedbackAssessment]): List of HealthCheck objects.

        Returns:
            HealthCheckStatistics: Calculated statistics in a structured format.
        """
        if len(health_checks) < 2:
            raise ValueError("At least 2 records are required to calculate meaningful statistics.")

        # Filter out None values
        efforts = [hc.effort for hc in health_checks if hc.effort is not None]
        impacts = [hc.impact for hc in health_checks if hc.impact is not None]
        morales = [hc.morale for hc in health_checks if hc.morale is not None]
        retentions = [hc.retention for hc in health_checks if hc.retention is not None]

        def calculate_stat(value: List[int], method: str) -> Optional[float]:
            if not value:
                return None
            if method == "mean":
                return round(statistics.mean(value), 2)
            elif method == "std_dev" and len(value) > 1:
                return round(statistics.stdev(value), 2)
            return None

        # Calculate means and standard deviations
        means = {
            "effort": calculate_stat(efforts, "mean"),
            "impact": calculate_stat(impacts, "mean"),
            "morale": calculate_stat(morales, "mean"),
            "retention": calculate_stat(retentions, "mean"),
        }

        std_devs = {
            "effort": calculate_stat(efforts, "std_dev"),
            "impact": calculate_stat(impacts, "std_dev"),
            "morale": calculate_stat(morales, "std_dev"),
            "retention": calculate_stat(retentions, "std_dev"),
        }

        def correlation(x: List[int], y: List[int]) -> Optional[float]:
            if len(x) != len(y) or len(x) < 2:
                return None
            mean_x, mean_y = statistics.mean(x), statistics.mean(y)
            numerator = sum((xi - mean_x) * (yi - mean_y) for xi, yi in zip(x, y))
            denominator = (
                sum((xi - mean_x) ** 2 for xi in x) * sum((yi - mean_y) ** 2 for yi in y)
            ) ** 0.5
            return round(numerator / denominator, 2) if denominator != 0 else None

        # Calculate correlations
        correlations = CorrelationSummary(
            effort_vs_impact=correlation(efforts, impacts) if efforts and impacts else None,
            effort_vs_morale=correlation(efforts, morales) if efforts and morales else None,
            effort_vs_retention=(
                correlation(efforts, retentions) if efforts and retentions else None
            ),
            impact_vs_morale=correlation(impacts, morales) if impacts and morales else None,
            impact_vs_retention=(
                correlation(impacts, retentions) if impacts and retentions else None
            ),
            morale_vs_retention=(
                correlation(morales, retentions) if morales and retentions else None
            ),
        )

        return HealthCheckStatistics(
            sample_size=len(health_checks),
            means=means,
            std_devs=std_devs,
            correlations=correlations,
        )
