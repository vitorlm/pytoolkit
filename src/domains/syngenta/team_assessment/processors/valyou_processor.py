import csv
import os
from pathlib import Path

from domains.syngenta.team_assessment.core.valyou import MemberRecognitions, ValYouRecognition
from utils.logging.logging_manager import LogManager
from utils.string_utils import StringUtils


class ValYouProcessor:
    """Processes Val-You CSV export files and extracts recognition data per team member.

    Parses the CSV, filters approved recognitions, and matches recipients
    to team members using case-insensitive, accent-stripped name comparison.
    """

    def __init__(self) -> None:
        self.logger = LogManager.get_instance().get_logger("ValYouProcessor")
        # Load ignored award reasons from environment
        ignored_reasons_str = os.getenv("VALYOU_IGNORED_AWARD_REASONS", "")
        self.ignored_award_reasons = (
            {reason.strip() for reason in ignored_reasons_str.split(",") if reason.strip()}
            if ignored_reasons_str
            else set()
        )
        if self.ignored_award_reasons:
            self.logger.info(
                f"Val-You filtering enabled: ignoring award reasons: {', '.join(sorted(self.ignored_award_reasons))}"
            )

    def process(
        self,
        csv_path: str,
        member_names: dict[str, tuple[str, str]],
    ) -> dict[str, MemberRecognitions]:
        """Process Val-You CSV and aggregate recognitions per team member.

        Args:
            csv_path: Path to the Val-You CSV export file.
            member_names: Dictionary mapping normalized first name (lowercase, no accents)
                to (original_first_name, original_last_name) tuple.

        Returns:
            Dictionary mapping member first name (original) to MemberRecognitions.
        """
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"Val-You CSV file not found: {csv_path}")

        self.logger.info(f"Processing Val-You CSV: {csv_path}")

        # Read and parse CSV
        rows = self._read_csv(csv_path)
        self.logger.info(f"Read {len(rows)} rows from CSV")

        # Filter approved recognitions only
        approved_rows = [row for row in rows if row.get("Status", "").strip() == "Aprovada"]
        self.logger.info(f"Filtered {len(approved_rows)} approved recognitions (out of {len(rows)} total)")

        # Filter out ignored award reasons
        if self.ignored_award_reasons:
            filtered_rows = [
                row for row in approved_rows if row.get("Award Reason", "").strip() not in self.ignored_award_reasons
            ]
            ignored_count = len(approved_rows) - len(filtered_rows)
            if ignored_count > 0:
                self.logger.info(
                    f"Filtered out {ignored_count} recognitions with ignored award reasons "
                    f"({', '.join(sorted(self.ignored_award_reasons))})"
                )
            approved_rows = filtered_rows

        # Match and aggregate recognitions per member
        member_recognitions: dict[str, list[ValYouRecognition]] = {}
        unmatched_recipients: set[str] = set()

        for row in approved_rows:
            recipient_first = row.get("Recipient First Name", "").strip()
            recipient_last = row.get("Recipient Last Name", "").strip()

            if not recipient_first:
                continue

            # Normalize recipient name for matching
            normalized_first = StringUtils.remove_accents(recipient_first).lower()
            normalized_last = StringUtils.remove_accents(recipient_last).lower()

            # Try to match by first name, then verify last name
            matched_key = self._match_member(normalized_first, normalized_last, member_names)

            if matched_key is None:
                full_recipient = f"{recipient_first} {recipient_last}".strip()
                unmatched_recipients.add(full_recipient)
                continue

            # Build recognition record
            recognition = ValYouRecognition(
                sender_first_name=row.get("Nominator First Name", "").strip(),
                sender_last_name=row.get("Nominator Last Name", "").strip(),
                sender_department=row.get("Nominator Department", "").strip(),
                sender_country=row.get("Nominator Country", "").strip(),
                status=row.get("Status", "").strip(),
                award_type=row.get("Award Type", "").strip(),
                award_reason=row.get("Award Reason", "").strip(),
                title=row.get("Title", "").strip(),
                message=row.get("Award Message", "").strip(),
                privacy=row.get("Award Privacy", "").strip(),
                points=self._parse_points(row.get("Value", "0")),
            )

            if matched_key not in member_recognitions:
                member_recognitions[matched_key] = []
            member_recognitions[matched_key].append(recognition)

        # Log unmatched
        if unmatched_recipients:
            self.logger.warning(
                f"Unmatched recipients ({len(unmatched_recipients)}): {', '.join(sorted(unmatched_recipients))}"
            )

        # Build aggregated MemberRecognitions
        results: dict[str, MemberRecognitions] = {}
        for member_key, recognitions in member_recognitions.items():
            senders = list({f"{r.sender_first_name} {r.sender_last_name}".strip() for r in recognitions})
            award_type_breakdown: dict[str, int] = {}
            award_reason_breakdown: dict[str, int] = {}

            for r in recognitions:
                if r.award_type:
                    award_type_breakdown[r.award_type] = award_type_breakdown.get(r.award_type, 0) + 1
                if r.award_reason:
                    award_reason_breakdown[r.award_reason] = award_reason_breakdown.get(r.award_reason, 0) + 1

            results[member_key] = MemberRecognitions(
                total_count=len(recognitions),
                recognitions=recognitions,
                senders=senders,
                award_type_breakdown=award_type_breakdown,
                award_reason_breakdown=award_reason_breakdown,
            )

            self.logger.info(f"  {member_key}: {len(recognitions)} recognitions from {len(senders)} senders")

        self.logger.info(
            f"Val-You processing complete: {sum(r.total_count for r in results.values())} "
            f"recognitions matched to {len(results)} members"
        )
        return results

    def _read_csv(self, csv_path: str) -> list[dict[str, str]]:
        """Read CSV file with encoding detection fallback.

        Args:
            csv_path: Path to the CSV file.

        Returns:
            List of dictionaries representing each CSV row.
        """
        # Try UTF-8-sig first (handles BOM), then fall back to others
        for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                with open(csv_path, encoding=encoding, newline="") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                    self.logger.debug(f"CSV read successfully with encoding: {encoding}")
                    return rows
            except UnicodeDecodeError:
                continue

        raise ValueError(f"Unable to read CSV file with any supported encoding: {csv_path}")

    def _match_member(
        self,
        normalized_first: str,
        normalized_last: str,
        member_names: dict[str, tuple[str, str]],
    ) -> str | None:
        """Match a recipient to a team member using normalized name comparison.

        Args:
            normalized_first: Normalized (lowercase, no accents) recipient first name.
            normalized_last: Normalized (lowercase, no accents) recipient last name.
            member_names: Dictionary mapping normalized first name to (original_first, original_last).

        Returns:
            The original first name of the matched member, or None.
        """
        if normalized_first in member_names:
            original_first, original_last = member_names[normalized_first]
            # If member has a last name, verify it matches
            if original_last:
                member_last_normalized = StringUtils.remove_accents(original_last).lower()
                if normalized_last and normalized_last != member_last_normalized:
                    return None
            return original_first

        return None

    def _parse_points(self, value: str) -> int:
        """Parse points value from CSV, handling empty/non-numeric values.

        Args:
            value: Points value as string from CSV.

        Returns:
            Parsed integer points value, or 0 if invalid.
        """
        try:
            return int(value.strip()) if value.strip() else 0
        except (ValueError, AttributeError):
            return 0
