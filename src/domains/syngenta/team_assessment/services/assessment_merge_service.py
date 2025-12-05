"""Service for merging assessment data across multiple periods."""

from pathlib import Path

from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class AssessmentMergeService:
    """Service to merge assessment data for team members across multiple periods."""

    def __init__(self, base_output_path: str | None = None):
        """Initialize the assessment merge service.

        Args:
            base_output_path: Base path for assessment folders. Defaults to ./output
        """
        self.logger = LogManager.get_instance().get_logger("AssessmentMergeService")
        self.base_path = Path(base_output_path) if base_output_path else Path("./output")

    def load_member_stats(self, folder_path: Path, first_name: str) -> dict | None:
        """Load statistics for a team member from a specific assessment period.

        Args:
            folder_path: Path to the assessment period folder
            first_name: First name of the member

        Returns:
            Dictionary with member stats or None if not found
        """
        member_folder = folder_path / "members" / first_name
        stats_file = member_folder / "stats.json"

        if stats_file.exists():
            self.logger.debug(f"Loading stats for {first_name} from {stats_file}")
            return JSONManager.read_json(str(stats_file))

        self.logger.warning(f"Stats file not found for {first_name} at {stats_file}")
        return None

    def calculate_member_average(self, stats: dict) -> float:
        """Calculate overall average score for a member.

        Args:
            stats: Member statistics dictionary

        Returns:
            Overall average score
        """
        scores = []

        if "feedback" in stats:
            for evaluator, categories in stats["feedback"].items():
                for category, items in categories.items():
                    if isinstance(items, list):
                        for item in items:
                            if "level" in item:
                                scores.append(item["level"])

        return round(sum(scores) / len(scores), 2) if scores else 0

    def calculate_category_averages(self, stats: dict) -> dict[str, float]:
        """Calculate average scores per category.

        Args:
            stats: Member statistics dictionary

        Returns:
            Dictionary mapping category names to average scores
        """
        category_scores = {}

        if "feedback" in stats:
            for evaluator, categories in stats["feedback"].items():
                for category, items in categories.items():
                    if isinstance(items, list):
                        if category not in category_scores:
                            category_scores[category] = []
                        for item in items:
                            if "level" in item:
                                category_scores[category].append(item["level"])

        return {category: round(sum(scores) / len(scores), 2) for category, scores in category_scores.items() if scores}

    def discover_assessment_periods(self) -> list[tuple[str, Path]]:
        """Automatically discover all assessment period folders.

        Returns:
            List of tuples (period_name, folder_path) sorted by date
        """
        assessment_folders = []

        for folder in sorted(self.base_path.glob("assessment_*")):
            parts = folder.name.replace("assessment_", "").split("_")
            if len(parts) == 2:
                month, year = parts
                period_name = f"{year}-{month.capitalize()}"
                assessment_folders.append((period_name, folder))
                self.logger.debug(f"Found assessment period: {period_name} at {folder}")

        self.logger.info(f"Discovered {len(assessment_folders)} assessment periods")
        return assessment_folders

    def merge_member_assessments(
        self,
        member_name: str,
        output_dir: Path | None = None,
        assessment_folders: list[tuple[str, Path]] | None = None,
    ) -> tuple[dict, Path]:
        """Merge assessment data for a specific member across periods.

        Args:
            member_name: Full name of the team member
            output_dir: Directory to save merged output. Defaults to ./output/merged_assessments
            assessment_folders: List of (period, folder) tuples. Auto-discovered if None

        Returns:
            Tuple of (merged_data, output_file_path)
        """
        first_name = member_name.split()[0]
        self.logger.info(f"Starting merge for {member_name} (first name: {first_name})")

        # Auto-discover periods if not specified
        if assessment_folders is None:
            assessment_folders = self.discover_assessment_periods()

        # Initialize merged data structure
        merged_data = {
            "member_name": member_name,
            "first_name": first_name,
            "assessments": [],
            "evolution": {"overall_avg": [], "categories": {}},
        }

        # Process each assessment period
        for period, folder_path in assessment_folders:
            if not folder_path.exists():
                self.logger.warning(f"Assessment folder not found: {folder_path}")
                continue

            stats = self.load_member_stats(folder_path, first_name)
            if stats:
                overall_avg = self.calculate_member_average(stats)
                category_avgs = self.calculate_category_averages(stats)

                assessment_data = {
                    "period": period,
                    "overall_average": overall_avg,
                    "category_averages": category_avgs,
                    "raw_feedback": stats.get("feedback", {}),
                }
                merged_data["assessments"].append(assessment_data)

                # Track evolution
                merged_data["evolution"]["overall_avg"].append({"period": period, "value": overall_avg})

                for category, avg in category_avgs.items():
                    if category not in merged_data["evolution"]["categories"]:
                        merged_data["evolution"]["categories"][category] = []
                    merged_data["evolution"]["categories"][category].append({"period": period, "value": avg})

                self.logger.debug(f"Processed period {period} - Overall avg: {overall_avg}")

        # Calculate summary statistics
        merged_data["summary"] = self._calculate_summary(merged_data)

        # Save to output file
        if output_dir is None:
            output_dir = self.base_path / "merged_assessments"

        FileManager.create_folder(str(output_dir))

        output_file = output_dir / f"{first_name}_merged_assessment.json"
        JSONManager.write_json(merged_data, str(output_file))

        self.logger.info(f"Merge completed. Saved to {output_file}")
        return merged_data, output_file

    def _calculate_summary(self, merged_data: dict) -> dict:
        """Calculate summary statistics for merged assessments.

        Args:
            merged_data: Merged assessment data

        Returns:
            Summary statistics dictionary
        """
        total_assessments = len(merged_data["assessments"])
        periods_covered = [a["period"] for a in merged_data["assessments"]]

        if total_assessments >= 2:
            first_val = merged_data["evolution"]["overall_avg"][0]["value"]
            last_val = merged_data["evolution"]["overall_avg"][-1]["value"]
            change = last_val - first_val
            change_pct = (change / first_val * 100) if first_val > 0 else 0

            return {
                "total_assessments": total_assessments,
                "periods_covered": periods_covered,
                "overall_trend": {
                    "first": first_val,
                    "last": last_val,
                    "change": round(change, 2),
                    "change_percentage": round(change_pct, 2),
                    "direction": "up" if change > 0 else "down" if change < 0 else "stable",
                },
            }
        else:
            return {
                "total_assessments": total_assessments,
                "periods_covered": periods_covered,
                "overall_trend": {"trend": "insufficient_data"},
            }
