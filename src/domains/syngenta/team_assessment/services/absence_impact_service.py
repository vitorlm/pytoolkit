"""Absence Impact Service - Calculate availability metrics and adjust productivity.

Created in Phase 4 to track member absences and normalize productivity metrics
by actual availability.

This service provides:
- Absence extraction from planning sheets
- Availability calculations
- Productivity normalization by availability
- Absence impact analysis
"""

from datetime import date, timedelta
from typing import Any

from utils.logging.logging_manager import LogManager

from ..core.assessment_report import AvailabilityMetrics, MemberAbsence


class AbsenceImpactService:
    """Service for tracking absences and calculating availability-adjusted metrics.

    Provides comprehensive absence tracking including:
    - Absence extraction from planning data
    - Availability percentage calculations
    - Productivity normalization by availability
    - Absence pattern analysis
    - Impact on team capacity
    """

    # Absence type mapping for different OFF codes
    ABSENCE_TYPES = {
        "off": "vacation",
        "vacation": "vacation",
        "sick": "sick_leave",
        "training": "training",
        "holiday": "public_holiday",
        "personal": "personal_day",
    }

    def __init__(self):
        """Initialize the service."""
        self.logger = LogManager.get_instance().get_logger("AbsenceImpactService")
        self.logger.info("AbsenceImpactService initialized")

    def extract_absences_from_planning(
        self,
        sheet_data: list[list[Any]],
        member_name: str,
        member_row_idx: int,
        date_columns: list[date],
        off_code: str = "off",
    ) -> list[MemberAbsence]:
        """Extract absence periods from planning sheet for a member.

        Scans the member's row in planning sheet to identify absence periods
        marked with OFF codes or similar indicators.

        Args:
            sheet_data: Planning sheet data
            member_name: Member name
            member_row_idx: Member's row index in sheet
            date_columns: List of dates corresponding to columns
            off_code: Code used to mark absences (default: "off")

        Returns:
            List of MemberAbsence records

        Example:
            >>> service = AbsenceImpactService()
            >>> absences = service.extract_absences_from_planning(
            ...     sheet_data,
            ...     "John Doe",
            ...     member_row_idx=15,
            ...     date_columns=[date(2024, 10, 1), date(2024, 10, 2), ...],
            ...     off_code="off"
            ... )
        """
        self.logger.info(f"Extracting absences for {member_name}")

        if member_row_idx >= len(sheet_data):
            self.logger.warning(f"Invalid row index {member_row_idx}")
            return []

        member_row = sheet_data[member_row_idx]
        absences = []

        # Track consecutive absence days
        absence_start: date | None = None
        absence_end: date | None = None
        absence_type: str | None = None

        for col_idx, cell_value in enumerate(member_row):
            if col_idx >= len(date_columns):
                break

            current_date = date_columns[col_idx]
            cell_str = str(cell_value).lower().strip() if cell_value else ""

            # Check if this cell marks an absence
            is_absence = False
            detected_type = None

            for key, absence_category in self.ABSENCE_TYPES.items():
                if key in cell_str:
                    is_absence = True
                    detected_type = absence_category
                    break

            if is_absence:
                # Continue or start new absence period
                if absence_start is None:
                    absence_start = current_date
                    absence_type = detected_type
                absence_end = current_date
            else:
                # End of absence period
                if absence_start and absence_end:
                    days_count = (absence_end - absence_start).days + 1
                    absences.append(
                        MemberAbsence(
                            member=member_name,
                            absence_type=absence_type or "unknown",
                            start_date=absence_start,
                            end_date=absence_end,
                            days_count=days_count,
                            notes="Extracted from planning sheet",
                        )
                    )
                    self.logger.debug(f"  Absence recorded: {absence_start} to {absence_end} ({days_count} days)")

                # Reset tracking
                absence_start = None
                absence_end = None
                absence_type = None

        # Handle absence period extending to end of planning
        if absence_start and absence_end:
            days_count = (absence_end - absence_start).days + 1
            absences.append(
                MemberAbsence(
                    member=member_name,
                    absence_type=absence_type or "unknown",
                    start_date=absence_start,
                    end_date=absence_end,
                    days_count=days_count,
                    notes="Extracted from planning sheet",
                )
            )

        self.logger.info(f"Found {len(absences)} absence periods for {member_name}")
        return absences

    def calculate_availability(
        self,
        member_name: str,
        period_start: date,
        period_end: date,
        absences: list[MemberAbsence],
        exclude_weekends: bool = True,
        exclude_holidays: list[date] | None = None,
    ) -> AvailabilityMetrics:
        """Calculate availability metrics for a member during a period.

        Args:
            member_name: Member name
            period_start: Period start date
            period_end: Period end date
            absences: List of member absences
            exclude_weekends: Whether to exclude weekends from working days
            exclude_holidays: List of public holidays to exclude

        Returns:
            AvailabilityMetrics with calculated rates

        Example:
            >>> service = AbsenceImpactService()
            >>> metrics = service.calculate_availability(
            ...     "John Doe",
            ...     date(2024, 10, 1),
            ...     date(2024, 12, 31),
            ...     absences_list
            ... )
        """
        self.logger.info(f"Calculating availability for {member_name}")
        self.logger.info(f"  Period: {period_start} to {period_end}")

        # Calculate total working days
        total_working_days = self._count_working_days(
            period_start, period_end, exclude_weekends, exclude_holidays or []
        )

        # Calculate days absent within the period
        days_absent = 0
        absences_by_type: dict[str, int] = {}

        for absence in absences:
            # Adjust absence dates to period boundaries
            adjusted_start = max(absence.start_date, period_start)
            adjusted_end = min(absence.end_date, period_end)

            # Skip absences outside period
            if adjusted_start > period_end or adjusted_end < period_start:
                continue

            # Count working days in absence period
            absence_working_days = self._count_working_days(
                adjusted_start, adjusted_end, exclude_weekends, exclude_holidays or []
            )

            days_absent += absence_working_days

            # Track by type
            absence_type = absence.absence_type
            if absence_type not in absences_by_type:
                absences_by_type[absence_type] = 0
            absences_by_type[absence_type] += absence_working_days

            self.logger.debug(f"  {absence_type}: {adjusted_start} to {adjusted_end} ({absence_working_days} days)")

        # Calculate availability percentage
        availability_percentage = (
            ((total_working_days - days_absent) / total_working_days * 100) if total_working_days > 0 else 0.0
        )

        self.logger.info(
            f"  Total working days: {total_working_days}, Days absent: {days_absent}, "
            f"Availability: {availability_percentage:.1f}%"
        )

        return AvailabilityMetrics(
            total_working_days=total_working_days,
            days_absent=days_absent,
            availability_percentage=availability_percentage,
            absences_by_type=absences_by_type,
        )

    def adjust_productivity_by_availability(
        self,
        raw_productivity: float,
        availability_percentage: float,
        normalization_method: str = "linear",
    ) -> float:
        """Adjust productivity metrics by availability percentage.

        Normalizes productivity to account for time member was actually available.

        Args:
            raw_productivity: Raw productivity metric (e.g., story points delivered)
            availability_percentage: Availability percentage (0-100)
            normalization_method: Method for adjustment ("linear" or "scaled")

        Returns:
            Adjusted productivity metric

        Example:
            >>> service = AbsenceImpactService()
            >>> # Member delivered 20 points but was only 80% available
            >>> adjusted = service.adjust_productivity_by_availability(20, 80.0)
            >>> # Result: 25.0 (normalized to full availability)
        """
        if availability_percentage <= 0 or availability_percentage > 100:
            self.logger.warning(f"Invalid availability percentage: {availability_percentage}%. Using raw value.")
            return raw_productivity

        if normalization_method == "linear":
            # Linear adjustment: normalize to 100% availability
            # If member was 80% available and delivered 20 points,
            # normalized productivity = 20 / 0.8 = 25 points
            adjusted = raw_productivity / (availability_percentage / 100.0)
        elif normalization_method == "scaled":
            # Scaled adjustment: proportional to availability loss
            # Less aggressive than linear
            availability_factor = availability_percentage / 100.0
            absence_impact = 1.0 - availability_factor
            adjusted = raw_productivity * (1.0 + absence_impact * 0.5)
        else:
            self.logger.warning(f"Unknown normalization method: {normalization_method}")
            adjusted = raw_productivity

        self.logger.debug(
            f"Productivity adjustment ({normalization_method}): "
            f"{raw_productivity:.2f} → {adjusted:.2f} (availability: {availability_percentage:.1f}%)"
        )

        return adjusted

    def analyze_absence_patterns(self, absences: list[MemberAbsence]) -> dict[str, Any]:
        """Analyze absence patterns to identify trends and anomalies.

        Args:
            absences: List of member absences

        Returns:
            Dict with pattern analysis
        """
        if not absences:
            return {
                "total_absences": 0,
                "total_days": 0,
                "avg_absence_duration": 0.0,
                "most_common_type": None,
                "patterns": {},
            }

        # Calculate statistics
        total_absences = len(absences)
        total_days = sum(a.days_count for a in absences)
        avg_duration = total_days / total_absences if total_absences > 0 else 0.0

        # Count by type
        type_counts: dict[str, int] = {}
        type_days: dict[str, int] = {}

        for absence in absences:
            atype = absence.absence_type
            type_counts[atype] = type_counts.get(atype, 0) + 1
            type_days[atype] = type_days.get(atype, 0) + absence.days_count

        most_common_type = max(type_counts, key=type_counts.get) if type_counts else None

        return {
            "total_absences": total_absences,
            "total_days": total_days,
            "avg_absence_duration": avg_duration,
            "most_common_type": most_common_type,
            "patterns": {
                "by_type_count": type_counts,
                "by_type_days": type_days,
            },
        }

    def calculate_team_capacity_impact(
        self,
        team_availability: dict[str, AvailabilityMetrics],
        team_size: int,
    ) -> dict[str, Any]:
        """Calculate impact of absences on overall team capacity.

        Args:
            team_availability: Dict mapping member name to their availability metrics
            team_size: Total team size

        Returns:
            Dict with team capacity analysis
        """
        if not team_availability:
            return {
                "team_availability_percentage": 100.0,
                "total_capacity_days_lost": 0,
                "members_below_80_percent": [],
                "high_impact_absences": [],
            }

        # Calculate team-level metrics
        total_working_days = sum(m.total_working_days for m in team_availability.values())
        total_days_absent = sum(m.days_absent for m in team_availability.values())

        team_availability_pct = (
            ((total_working_days - total_days_absent) / total_working_days * 100) if total_working_days > 0 else 100.0
        )

        # Identify members with low availability
        members_below_80 = [
            name for name, metrics in team_availability.items() if metrics.availability_percentage < 80.0
        ]

        # Identify high-impact absences (>5% of team capacity)
        capacity_threshold = total_working_days * 0.05
        high_impact = [
            {
                "member": name,
                "days_absent": metrics.days_absent,
                "percentage": metrics.availability_percentage,
            }
            for name, metrics in team_availability.items()
            if metrics.days_absent > capacity_threshold
        ]

        self.logger.info("Team capacity impact analysis:")
        self.logger.info(f"  Team availability: {team_availability_pct:.1f}%")
        self.logger.info(f"  Total days lost: {total_days_absent}")
        self.logger.info(f"  Members below 80%: {len(members_below_80)}")

        return {
            "team_availability_percentage": team_availability_pct,
            "total_capacity_days_lost": total_days_absent,
            "members_below_80_percent": members_below_80,
            "high_impact_absences": high_impact,
        }

    def _count_working_days(
        self,
        start_date: date,
        end_date: date,
        exclude_weekends: bool,
        holidays: list[date],
    ) -> int:
        """Count working days between two dates.

        Args:
            start_date: Start date
            end_date: End date
            exclude_weekends: Whether to exclude weekends
            holidays: List of public holidays

        Returns:
            Number of working days
        """
        if start_date > end_date:
            return 0

        working_days = 0
        current = start_date
        holiday_set = set(holidays)

        while current <= end_date:
            # Skip weekends if requested
            if exclude_weekends and current.weekday() >= 5:  # Saturday=5, Sunday=6
                current += timedelta(days=1)
                continue

            # Skip holidays
            if current in holiday_set:
                current += timedelta(days=1)
                continue

            working_days += 1
            current += timedelta(days=1)

        return working_days
