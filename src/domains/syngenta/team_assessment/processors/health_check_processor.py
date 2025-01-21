from pathlib import Path
from typing import Dict, List, Union, Any, Optional
from datetime import datetime
from utils.base_processor import BaseProcessor
from ..core.statistics import StatisticsHelper
from ..core.health_check import (
    FeedbackAssessment,
    CorrelationSummary,
    HealthCheckStatistics,
    MemberHealthCheck,
)
from ..core.validations import ValidationHelper
from utils.data.excel_manager import ExcelManager
from utils.file_manager import FileManager


class HealthCheckProcessor(BaseProcessor):
    """
    Processor for extracting Health Check data from Excel files.
    """

    def __init__(self):
        super().__init__(allowed_extensions=[".xlsx"])

    def process_file(self, file_path: Union[str, Path]) -> Dict[str, MemberHealthCheck]:
        """
        Processes a single file to extract Health Check data.

        Args:
            file_path (Union[str, Path]): Path to the Excel file.

        Returns:
            Dict[str, MemberHealthCheck]: Health check data grouped by members.
        """
        self.logger.info(f"Starting processing of file: {file_path}")
        file_path = Path(file_path)

        FileManager.validate_file(file_path, allowed_extensions=[".xlsx"])

        relevant_sheets = ExcelManager.filter_sheets_by_pattern(
            file_path, pattern="^(?!Example$|Data$)"
        )

        members_health_check: Dict[str, MemberHealthCheck] = {}
        for sheet_name in relevant_sheets:
            member_name = sheet_name.split(" ")[0]
            self.logger.info(f"Processing sheet: {sheet_name} for member: {member_name}")
            sheet_data = ExcelManager.read_excel_as_list(file_path, sheet_name=sheet_name)
            feedback_data = self.process_cycle(sheet_data)

            # Validate feedback data
            ValidationHelper.validate_health_check_data(feedback_data)

            # Calculate statistics if there are enough records
            statistics = (
                self._calculate_statistics(feedback_data) if len(feedback_data) > 3 else None
            )

            members_health_check[member_name] = MemberHealthCheck(
                feedback_data=feedback_data,
                statistics=statistics,
            )

        self.logger.info(f"Completed processing of file: {file_path}")
        return members_health_check

    def process_cycle(self, sheet_data: List[List[Any]]) -> List[FeedbackAssessment]:
        """
        Processes a single sheet to extract feedback data.

        Args:
            sheet_data (List[List[Any]]): Sheet data as a list of rows.

        Returns:
            List[FeedbackAssessment]: List of feedback assessments.
        """
        self.logger.debug("Extracting header from sheet data.")
        header = sheet_data[0]
        header_map = {col.lower(): idx for idx, col in enumerate(header) if col}

        feedback_data = []
        for row in sheet_data[1:]:
            if all(cell is None for cell in row):
                continue

            feedback_data.append(
                FeedbackAssessment(
                    health_check_date=self._parse_date(row[header_map["date"]]),
                    feedback_by=row[header_map.get("feedback by")],
                    effort=self._parse_optional_int(row[header_map.get("effort")]),
                    effort_comment=row[header_map.get("effort comment")],
                    impact=self._parse_optional_int(row[header_map.get("impact")]),
                    impact_comment=row[header_map.get("impact comment")],
                    morale=self._parse_optional_int(row[header_map.get("morale")]),
                    morale_comment=row[header_map.get("morale comment")],
                    retention=self._parse_optional_int(row[header_map.get("retention")]),
                    retention_comment=row[header_map.get("retention comment")],
                )
            )
        self.logger.info(f"Extracted {len(feedback_data)} feedback entries from sheet.")
        return feedback_data

    def _calculate_statistics(
        self, feedback_data: List[FeedbackAssessment]
    ) -> HealthCheckStatistics:
        """
        Calculates statistics for the feedback data.

        Args:
            feedback_data (List[FeedbackAssessment]): List of feedback assessments.

        Returns:
            HealthCheckStatistics: Calculated statistics.
        """
        self.logger.debug("Calculating statistics for feedback data.")
        efforts = [f.effort for f in feedback_data if f.effort is not None]
        impacts = [f.impact for f in feedback_data if f.impact is not None]
        morales = [f.morale for f in feedback_data if f.morale is not None]
        retentions = [f.retention for f in feedback_data if f.retention is not None]

        means = {
            "effort": StatisticsHelper.calculate_mean(efforts),
            "impact": StatisticsHelper.calculate_mean(impacts),
            "morale": StatisticsHelper.calculate_mean(morales),
            "retention": StatisticsHelper.calculate_mean(retentions),
        }

        std_devs = {
            "effort": StatisticsHelper.calculate_std_dev(efforts),
            "impact": StatisticsHelper.calculate_std_dev(impacts),
            "morale": StatisticsHelper.calculate_std_dev(morales),
            "retention": StatisticsHelper.calculate_std_dev(retentions),
        }

        correlations = CorrelationSummary(
            effort_vs_impact=StatisticsHelper.calculate_correlation(efforts, impacts),
            effort_vs_morale=StatisticsHelper.calculate_correlation(efforts, morales),
            effort_vs_retention=StatisticsHelper.calculate_correlation(efforts, retentions),
            impact_vs_morale=StatisticsHelper.calculate_correlation(impacts, morales),
            impact_vs_retention=StatisticsHelper.calculate_correlation(impacts, retentions),
            morale_vs_retention=StatisticsHelper.calculate_correlation(morales, retentions),
        )

        self.logger.info("Statistics calculation completed.")
        return HealthCheckStatistics(
            sample_size=len(feedback_data),
            means=means,
            std_devs=std_devs,
            correlations=correlations,
        )

    @staticmethod
    def _parse_date(date_value: Any) -> str:
        if isinstance(date_value, datetime):
            return date_value.strftime("%d/%m/%Y")
        return datetime.strptime(date_value, "%Y-%m-%d").strftime("%d/%m/%Y")

    @staticmethod
    def _parse_optional_int(value: Any) -> Optional[int]:
        return value if isinstance(value, int) else None
