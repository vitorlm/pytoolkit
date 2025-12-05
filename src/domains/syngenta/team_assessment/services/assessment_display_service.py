"""Service for displaying merged assessment data with formatted output."""

from pathlib import Path

from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager


class AssessmentDisplayService:
    """Service to display merged assessment data in a formatted way."""

    def __init__(self):
        """Initialize the assessment display service."""
        self.logger = LogManager.get_instance().get_logger("AssessmentDisplayService")

    def load_merged_assessment(self, member_name: str, merged_dir: Path | None = None) -> dict | None:
        """Load merged assessment data for a member.

        Args:
            member_name: Full name of the member
            merged_dir: Directory containing merged files. Defaults to ./output/merged_assessments

        Returns:
            Merged assessment data or None if not found
        """
        first_name = member_name.split()[0]

        if merged_dir is None:
            merged_dir = Path("./output/merged_assessments")

        file_path = merged_dir / f"{first_name}_merged_assessment.json"

        if not file_path.exists():
            self.logger.error(f"Merged assessment file not found: {file_path}")
            return None

        self.logger.info(f"Loading merged assessment from {file_path}")
        return JSONManager.read_json(str(file_path))

    def calculate_evaluator_average(self, feedback_data: dict) -> float:
        """Calculate average score from an evaluator's feedback.

        Args:
            feedback_data: Feedback data from one evaluator

        Returns:
            Average score
        """
        all_levels = []
        for category_data in feedback_data.values():
            if isinstance(category_data, list):
                for indicator in category_data:
                    if "level" in indicator:
                        all_levels.append(indicator["level"])
        return sum(all_levels) / len(all_levels) if all_levels else 0

    def format_period_details(self, assessment: dict, member_name: str) -> str:
        """Format details for a single assessment period.

        Args:
            assessment: Assessment data for one period
            member_name: Full name of the member (to identify self-evaluation)

        Returns:
            Formatted string with period details
        """
        output = []
        output.append(f"\n📅 {assessment['period']}")
        output.append(f"   Média geral: {assessment['overall_average']:.2f}")

        # Check for self-evaluation
        has_self = member_name in assessment.get("raw_feedback", {})
        output.append(f"   Self-evaluation: {'✅' if has_self else '❌'}")
        output.append(f"   Avaliadores: {len(assessment.get('raw_feedback', {}))}")

        # Categories
        output.append("   Critérios:")
        for crit, avg in sorted(assessment["category_averages"].items()):
            output.append(f"      • {crit}: {avg:.2f}")

        # Evaluators
        output.append("   Avaliadores:")
        for evaluator_name in sorted(assessment.get("raw_feedback", {}).keys()):
            eval_type = "[SELF]" if evaluator_name == member_name else "[PEER]"
            feedback = assessment["raw_feedback"][evaluator_name]
            avg = self.calculate_evaluator_average(feedback)
            output.append(f"      • {evaluator_name:25s} {eval_type} → {avg:.2f}")

        return "\n".join(output)

    def format_evolution(self, data: dict) -> str:
        """Format evolution section showing changes between periods.

        Args:
            data: Merged assessment data

        Returns:
            Formatted string with evolution details
        """
        output = []
        assessments = data["assessments"]

        for i in range(len(assessments) - 1):
            curr = assessments[i]
            next_period = assessments[i + 1]

            output.append(f"\n{curr['period']} → {next_period['period']}")

            # Overall trend
            diff = next_period["overall_average"] - curr["overall_average"]
            arrow = "📈" if diff > 0 else "📉" if diff < 0 else "➡️"
            output.append(
                f"   {arrow} Overall: {diff:+.2f} "
                f"({curr['overall_average']:.2f} → {next_period['overall_average']:.2f})"
            )

            # Categories that increased
            increased = []
            for crit in curr["category_averages"].keys():
                if crit in next_period["category_averages"]:
                    diff_crit = next_period["category_averages"][crit] - curr["category_averages"][crit]
                    if diff_crit > 0.01:  # threshold to avoid noise
                        increased.append((crit, diff_crit))

            if increased:
                for crit, diff_crit in sorted(increased, key=lambda x: x[1], reverse=True):
                    output.append(f"   📈 {crit}: {diff_crit:+.2f}")

            # Categories that decreased
            decreased = []
            for crit in curr["category_averages"].keys():
                if crit in next_period["category_averages"]:
                    diff_crit = next_period["category_averages"][crit] - curr["category_averages"][crit]
                    if diff_crit < -0.01:  # threshold to avoid noise
                        decreased.append((crit, diff_crit))

            if decreased:
                for crit, diff_crit in sorted(decreased, key=lambda x: x[1]):
                    output.append(f"   📉 {crit}: {diff_crit:+.2f}")

            # Stable categories
            stable = []
            for crit in curr["category_averages"].keys():
                if crit in next_period["category_averages"]:
                    diff_crit = next_period["category_averages"][crit] - curr["category_averages"][crit]
                    if abs(diff_crit) <= 0.01:
                        stable.append(crit)

            if stable:
                output.append(f"   ➡️  Estável: {', '.join(stable)}")

        return "\n".join(output)

    def display_merged_assessment(self, member_name: str, merged_dir: Path | None = None) -> str:
        """Display complete merged assessment for a member.

        Args:
            member_name: Full name of the member
            merged_dir: Directory containing merged files

        Returns:
            Formatted output string
        """
        data = self.load_merged_assessment(member_name, merged_dir)

        if not data:
            return f"❌ Arquivo de merge não encontrado para {member_name}"

        output = []

        # Header
        output.append("=" * 80)
        output.append(f"📊 MERGE COMPLETO - {data['member_name']}")
        output.append("=" * 80)

        # Period details
        for assessment in data["assessments"]:
            output.append(self.format_period_details(assessment, data["member_name"]))

        # Evolution section
        if len(data["assessments"]) > 1:
            output.append(f"\n{'=' * 80}")
            output.append("📈 EVOLUÇÃO")
            output.append("=" * 80)
            output.append(self.format_evolution(data))

        output.append("\n" + "=" * 80)

        return "\n".join(output)
