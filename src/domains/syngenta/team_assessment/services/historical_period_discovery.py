"""Historical Period Discovery Service

Automatically discovers historical evaluation periods for comparison analysis.
Given a current feedback folder path, discovers all previous evaluation periods
in a structured manner.
"""

import os
import re
from dataclasses import dataclass
from datetime import datetime

from utils.logging.logging_manager import LogManager


@dataclass
class EvaluationPeriod:
    """Represents a single evaluation period with metadata"""

    year: int
    period_name: str  # e.g., "Nov", "Jun"
    folder_path: str
    timestamp: datetime  # For sorting
    is_current: bool = False

    def __repr__(self):
        return f"{self.year}/{self.period_name}"


class HistoricalPeriodDiscovery:
    """Service to discover historical evaluation periods automatically"""

    # Common period names and their month mapping for timestamp
    PERIOD_MONTH_MAP = {
        "jan": 1,
        "feb": 2,
        "mar": 3,
        "apr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "aug": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dec": 12,
    }

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("HistoricalPeriodDiscovery")

    def validate_feedback_folder_structure(self, feedback_folder: str) -> None:
        """Validates that the feedback folder has the expected structure.

        Required format: .../Evaluation process/YEAR/MONTH/
        where MONTH is a 3-letter English month abbreviation (Jan, Feb, Mar, etc.)

        Raises:
            FileNotFoundError: If folder doesn't exist
            ValueError: If folder structure is invalid
        """
        # Check if folder exists
        if not os.path.exists(feedback_folder):
            raise FileNotFoundError(
                f"Feedback folder not found: {feedback_folder}\n\n"
                f"Expected structure:\n"
                f"  .../Evaluation process/YEAR/MONTH/\n\n"
                f"Examples:\n"
                f"  /Users/user/OneDrive/Evaluation process/2025/Nov/\n"
                f"  /Users/user/OneDrive/Evaluation process/2024/Jun/\n"
                f"  /Users/user/OneDrive/Evaluation process/2024/Dec/"
            )

        # Parse the path to check structure
        normalized = os.path.normpath(feedback_folder)
        parts = normalized.split(os.sep)

        # Find "Evaluation process" or similar directory
        base_idx = -1
        for idx, part in enumerate(parts):
            # More flexible matching - "evaluation" or "avalia" covers both languages
            part_lower = part.lower()
            if "evaluation" in part_lower or "avalia" in part_lower:
                base_idx = idx
                break

        if base_idx == -1:
            raise ValueError(
                f"Invalid feedback folder structure: {feedback_folder}\n\n"
                f"Expected structure must contain an 'Evaluation process' folder:\n"
                f"  .../Evaluation process/YEAR/MONTH/\n\n"
                f"Your path: {feedback_folder}\n"
                f"Path parts: {' > '.join(parts[-5:] if len(parts) >= 5 else parts)}\n\n"
                f"Examples of valid paths:\n"
                f"  /Users/user/OneDrive/Evaluation process/2025/Nov/\n"
                f"  /Users/user/OneDrive/Evaluation process/2024/Jun/"
            )

        # Validate we have YEAR and MONTH after base
        year_idx = base_idx + 1
        month_idx = base_idx + 2

        if len(parts) <= year_idx:
            raise ValueError(
                f"Invalid feedback folder structure: {feedback_folder}\n\n"
                f"After the 'Evaluation process' folder, you must have YEAR/MONTH/.\n\n"
                f"Expected: .../{parts[base_idx]}/YEAR/MONTH/\n"
                f"Found: {feedback_folder}\n\n"
                f"Examples:\n"
                f"  .../{parts[base_idx]}/2025/Nov/\n"
                f"  .../{parts[base_idx]}/2024/Jun/"
            )

        # Validate year is numeric
        try:
            year = int(parts[year_idx])
            if year < 2020 or year > 2030:
                raise ValueError(f"Year {year} seems invalid (expected 2020-2030)")
        except (ValueError, IndexError) as e:
            raise ValueError(
                f"Invalid year in feedback folder structure: {feedback_folder}\n\n"
                f"After '/{parts[base_idx]}/' you must have a valid YEAR (numeric).\n\n"
                f"Expected: .../{parts[base_idx]}/YEAR/MONTH/\n"
                f"Found: .../{parts[base_idx]}/{parts[year_idx] if len(parts) > year_idx else '???'}/\n\n"
                f"Error: {e}"
            )

        # Validate MONTH exists and is 3-letter abbreviation
        if len(parts) <= month_idx:
            raise ValueError(
                f"Invalid feedback folder structure: {feedback_folder}\n\n"
                f"Missing MONTH folder after YEAR.\n\n"
                f"Expected: .../{parts[base_idx]}/{year}/MONTH/\n"
                f"Found: {feedback_folder}\n\n"
                f"Valid MONTH values (3 letters): Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec"
            )

        month_name = parts[month_idx]
        valid_months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]

        if len(month_name) != 3 or month_name.lower() not in valid_months:
            raise ValueError(
                f"Invalid MONTH in feedback folder structure: {feedback_folder}\n\n"
                f"MONTH must be a 3-letter English month abbreviation.\n\n"
                f"Found: '{month_name}'\n"
                f"Expected one of: Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec\n\n"
                f"Full path structure: .../{parts[base_idx]}/{year}/MONTH/"
            )

    def discover_periods(self, current_feedback_folder: str) -> list[EvaluationPeriod]:
        """Discovers all evaluation periods including current and historical ones.

        Args:
            current_feedback_folder: Path to current feedback folder (e.g., ".../2025/Nov")

        Returns:
            List of EvaluationPeriod sorted chronologically (oldest to newest)
        """
        self.logger.info(f"Discovering periods from: {current_feedback_folder}")

        # Extract components from current path
        current_year, current_period, base_path = self._parse_feedback_path(current_feedback_folder)

        if not current_year or not current_period or not base_path:
            self.logger.warning("Could not parse feedback folder structure. Using only current period.")
            return [
                EvaluationPeriod(
                    year=datetime.now().year,
                    period_name=os.path.basename(current_feedback_folder),
                    folder_path=current_feedback_folder,
                    timestamp=datetime.now(),
                    is_current=True,
                )
            ]

        self.logger.info(f"Current period: {current_year}/{current_period}, Base: {base_path}")

        # Discover all periods
        all_periods = self._scan_all_periods(base_path, current_year, current_period)

        # Sort chronologically
        all_periods.sort(key=lambda p: p.timestamp)

        self.logger.info(f"Discovered {len(all_periods)} periods: " + ", ".join([str(p) for p in all_periods]))

        return all_periods

    def _parse_feedback_path(self, feedback_folder: str) -> tuple[int | None, str | None, str | None]:
        """Parses feedback folder path to extract year, month, and base path.

        Expected structure: .../Evaluation process/YEAR/MONTH/
        where MONTH is a 3-letter English month abbreviation

        Returns:
            Tuple of (year, month_name, base_path)
        """
        normalized = os.path.normpath(feedback_folder)
        parts = normalized.split(os.sep)

        try:
            # Find "Evaluation process" or similar base directory
            base_idx = -1
            for idx, part in enumerate(parts):
                part_lower = part.lower()
                if "evaluation" in part_lower or "avalia" in part_lower:
                    base_idx = idx
                    break

            if base_idx == -1:
                return None, None, None

            # Extract year and month
            year_idx = base_idx + 1
            month_idx = base_idx + 2

            if len(parts) <= month_idx:
                return None, None, None

            year = int(parts[year_idx])
            month_name = parts[month_idx]
            base_path = os.sep.join(parts[:year_idx])

            return year, month_name, base_path

        except (ValueError, IndexError) as e:
            self.logger.error(f"Error parsing feedback path: {e}")
            return None, None, None

    def _scan_all_periods(self, base_path: str, current_year: int, current_period: str) -> list[EvaluationPeriod]:
        """Scans base directory for all evaluation periods across years.

        Args:
            base_path: Base directory containing year folders
            current_year: Year of current evaluation
            current_period: Period name of current evaluation

        Returns:
            List of discovered EvaluationPeriod objects
        """
        periods = []

        if not os.path.exists(base_path):
            self.logger.warning(f"Base path does not exist: {base_path}")
            return periods

        # Scan for year directories (e.g., 2023, 2024, 2025)
        for entry in os.listdir(base_path):
            year_path = os.path.join(base_path, entry)

            if not os.path.isdir(year_path):
                continue

            # Check if directory name is a valid year
            if not re.match(r"^\d{4}$", entry):
                continue

            year = int(entry)

            # Skip years before 2024 (old format/structure)
            if year < 2024:
                self.logger.debug(f"Skipping year {year} - only processing 2024 onwards")
                continue

            # Scan for period directories within year (e.g., Nov, Jun)
            for period_entry in os.listdir(year_path):
                period_path = os.path.join(year_path, period_entry)

                if not os.path.isdir(period_path):
                    continue

                # Skip non-period directories (like "Competencies Matrix" files)
                if not self._is_valid_period_name(period_entry):
                    continue

                # Check if directory contains evaluation files
                if not self._contains_evaluation_files(period_path):
                    self.logger.debug(f"Skipping {year}/{period_entry} - no evaluation files found")
                    continue

                # Create timestamp for sorting
                timestamp = self._create_timestamp(year, period_entry)

                # Check if this is the current period
                is_current = year == current_year and period_entry == current_period

                periods.append(
                    EvaluationPeriod(
                        year=year,
                        period_name=period_entry,
                        folder_path=period_path,
                        timestamp=timestamp,
                        is_current=is_current,
                    )
                )

        return periods

    def _is_valid_period_name(self, name: str) -> bool:
        """Checks if directory name looks like a valid evaluation period.

        Valid examples: Nov, Jun, May, Q1, Q2, etc.
        """
        name_lower = name.lower()

        # Check for month names
        if name_lower in self.PERIOD_MONTH_MAP:
            return True

        # Check for quarter patterns (Q1, Q2, etc.)
        if re.match(r"^q[1-4]$", name_lower):
            return True

        # Check for date patterns (YYYY-MM, etc.)
        if re.match(r"^\d{4}-\d{2}$", name):
            return True

        return False

    def _contains_evaluation_files(self, period_path: str) -> bool:
        """Checks if directory contains evaluation Excel files.

        Returns:
            True if at least one .xlsx file exists
        """
        try:
            files = os.listdir(period_path)
            return any(f.endswith(".xlsx") for f in files)
        except (OSError, PermissionError):
            return False

    def _create_timestamp(self, year: int, period_name: str) -> datetime:
        """Creates a datetime timestamp for sorting periods.

        Args:
            year: Year number
            period_name: Period name (e.g., "Nov", "Jun")

        Returns:
            datetime object for chronological sorting
        """
        period_lower = period_name.lower()

        # Try to map period name to month
        month = self.PERIOD_MONTH_MAP.get(period_lower[:3], 12)

        # For quarters, estimate middle month
        if period_lower.startswith("q"):
            quarter = int(period_lower[1])
            month = quarter * 3 - 1  # Q1->Feb, Q2->May, Q3->Aug, Q4->Nov

        return datetime(year, month, 1)

    def get_historical_periods(self, current_feedback_folder: str) -> list[EvaluationPeriod]:
        """Gets only historical periods (excluding current).

        Args:
            current_feedback_folder: Path to current feedback folder

        Returns:
            List of historical periods sorted chronologically
        """
        all_periods = self.discover_periods(current_feedback_folder)
        historical = [p for p in all_periods if not p.is_current]

        self.logger.info(f"Found {len(historical)} historical periods")
        return historical

    def get_current_period(self, current_feedback_folder: str) -> EvaluationPeriod | None:
        """Gets the current evaluation period.

        Args:
            current_feedback_folder: Path to current feedback folder

        Returns:
            EvaluationPeriod for current period or None
        """
        all_periods = self.discover_periods(current_feedback_folder)
        current = next((p for p in all_periods if p.is_current), None)

        if current:
            self.logger.info(f"Current period: {current}")
        else:
            self.logger.warning("Could not identify current period")

        return current
