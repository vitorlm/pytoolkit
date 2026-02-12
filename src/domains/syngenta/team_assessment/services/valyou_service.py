from pathlib import Path

from domains.syngenta.team_assessment.core.member import Member
from domains.syngenta.team_assessment.core.valyou import MemberRecognitions
from domains.syngenta.team_assessment.processors.valyou_processor import ValYouProcessor
from utils.logging.logging_manager import LogManager
from utils.string_utils import StringUtils


class ValYouService:
    """Service for fetching and processing Val-You recognition data.

    Validates the CSV file, builds the member name lookup, and delegates
    CSV parsing to ValYouProcessor.
    """

    def __init__(self) -> None:
        self.logger = LogManager.get_instance().get_logger("ValYouService")
        self.processor = ValYouProcessor()

    def fetch_recognitions(
        self,
        csv_path: str,
        members: dict[str, Member],
    ) -> dict[str, MemberRecognitions]:
        """Fetch and process Val-You recognitions from the CSV export.

        Args:
            csv_path: Path to the Val-You CSV export file.
            members: Dictionary of team members keyed by first name.

        Returns:
            Dictionary mapping member first name to MemberRecognitions.

        Raises:
            FileNotFoundError: If the CSV file does not exist.
            ValueError: If the CSV file is malformed or unreadable.
        """
        # Validate file exists
        path = Path(csv_path)
        if not path.exists():
            raise FileNotFoundError(f"Val-You CSV file not found: {csv_path}")
        if not path.is_file():
            raise ValueError(f"Val-You path is not a file: {csv_path}")

        self.logger.info(f"Processing Val-You recognitions from: {csv_path}")
        self.logger.info(f"Matching against {len(members)} team members")

        # Build member name lookup: normalized_first_name -> (original_first, original_last)
        member_names = self._build_member_lookup(members)

        # Delegate to processor
        try:
            results = self.processor.process(csv_path, member_names)
        except Exception as e:
            self.logger.error(f"Failed to process Val-You CSV: {e}", exc_info=True)
            raise

        self.logger.info(
            f"Val-You processing complete: {sum(r.total_count for r in results.values())} "
            f"recognitions for {len(results)} members"
        )
        return results

    def _build_member_lookup(self, members: dict[str, Member]) -> dict[str, tuple[str, str]]:
        """Build a normalized name lookup from the members dictionary.

        Args:
            members: Dictionary of Member objects keyed by first name.

        Returns:
            Dictionary mapping normalized first name (lowercase, no accents)
            to (original_first_name, original_last_name) tuple.
        """
        lookup: dict[str, tuple[str, str]] = {}
        for member in members.values():
            normalized_first = StringUtils.remove_accents(member.name).lower()
            last_name = member.last_name or ""
            lookup[normalized_first] = (member.name, last_name)

        self.logger.debug(f"Built member lookup with {len(lookup)} entries")
        return lookup
