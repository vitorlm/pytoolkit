from pathlib import Path

from domains.syngenta.team_assessment.core.member import Member
from domains.syngenta.team_assessment.core.workday import WorkdayData, WorkdayFeedback
from domains.syngenta.team_assessment.processors.workday_processor import WorkdayProcessor
from utils.logging.logging_manager import LogManager
from utils.string_utils import StringUtils


class WorkdayService:
    """Service for processing Workday evaluation and feedback data."""

    def __init__(self) -> None:
        self.logger = LogManager.get_instance().get_logger("WorkdayService")
        self.processor = WorkdayProcessor()

    def process_workday_folder(self, workday_root: str, members: dict[str, Member]) -> dict[str, WorkdayData]:
        """Iterates through member subfolders in the Workday root and parses PDFs.

        Args:
            workday_root: Path to the Workday root folder (contains member subfolders).
            members: Dictionary of team members keyed by first name.

        Returns:
            Dictionary mapping member first name to WorkdayData.
        """
        root_path = Path(workday_root)
        if not root_path.exists() or not root_path.is_dir():
            self.logger.warning(f"Workday root folder not found: {workday_root}")
            return {}

        self.logger.info(f"Processing Workday data from: {workday_root}")
        results: dict[str, WorkdayData] = {}

        # Iterate subdirectories (each directory = one member)
        for member_folder in root_path.iterdir():
            if not member_folder.is_dir():
                continue

            # Skip hidden folders
            if member_folder.name.startswith("."):
                continue

            member_name = self._resolve_member_name(member_folder.name, members)
            if not member_name:
                self.logger.warning(f"Could not resolve member for folder: {member_folder.name}")
                continue

            self.logger.info(f"Processing Workday data for member: {member_name}")
            workday_data = WorkdayData()

            # 1. Process evaluation PDF
            eval_pdf = member_folder / "avaliacao-what-how.pdf"
            if eval_pdf.exists():
                workday_data.ratings = self.processor.parse_evaluation_pdf(str(eval_pdf))
            else:
                self.logger.debug(f"Evaluation PDF missing for {member_name} in {member_folder}")

            # 2. Process feedback subfolder
            feedback_folder = member_folder / "feedback"
            if feedback_folder.exists() and feedback_folder.is_dir():
                for feedback_file in feedback_folder.glob("*.pdf"):
                    text = self.processor.parse_feedback_pdf(str(feedback_file))
                    authors = self.processor.parse_authors_from_filename(feedback_file.name)

                    if text or authors:
                        feedback_entry = WorkdayFeedback(authors=authors, text=text)
                        workday_data.official_feedback.append(feedback_entry)

            results[member_name] = workday_data

        self.logger.info(f"Workday processing complete: {len(results)} members processed")
        return results

    def _resolve_member_name(self, folder_name: str, members: dict[str, Member]) -> str | None:
        """Resolves a folder name to a member first name.

        Uses normalization and first name matching, with fuzzy fallback.
        """
        # Normalize folder name
        normalized_folder = StringUtils.remove_accents(folder_name).lower()

        # 1. Try exact match on first part
        folder_parts = normalized_folder.split()
        if not folder_parts:
            return None

        folder_first_name = folder_parts[0]

        # Check against member first names
        for first_name in members.keys():
            normalized_member_name = StringUtils.remove_accents(first_name).lower()
            if normalized_member_name == folder_first_name:
                return first_name

        # 2. Try fuzzy match as fallback
        best_match = None
        highest_score = 0.0

        for first_name in members.keys():
            normalized_member_name = StringUtils.remove_accents(first_name).lower()
            score = StringUtils.compare_words(normalized_member_name, folder_first_name)
            if score > highest_score:
                highest_score = score
                best_match = first_name

        if highest_score > 0.8:  # threshold for fuzzy match
            self.logger.debug(
                f"Fuzzy matched folder '{folder_name}' to member '{best_match}' (score: {highest_score:.2f})"
            )
            return best_match

        return None
