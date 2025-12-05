"""Command to merge team member assessments across multiple periods."""

from argparse import ArgumentParser, Namespace
from pathlib import Path

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

from .services.assessment_merge_service import AssessmentMergeService


class MergeAssessmentCommand(BaseCommand):
    """Command to merge assessment data for a team member across multiple periods."""

    @staticmethod
    def get_name() -> str:
        return "merge-assessment"

    @staticmethod
    def get_description() -> str:
        return "Merge assessment data for a team member across multiple evaluation periods."

    @staticmethod
    def get_help() -> str:
        return """
        Merge assessment data for a team member across multiple evaluation periods.
        
        This command automatically discovers all assessment period folders and merges
        the data for a specific team member, calculating:
        - Overall averages per period
        - Category-level averages per period
        - Evolution trends across periods
        - Summary statistics
        
        Examples:
            # Merge assessments for a specific member
            python src/main.py syngenta team_assessment merge-assessment --member-name "Italo Ortega"
            
            # Merge with custom output directory
            python src/main.py syngenta team_assessment merge-assessment \\
                --member-name "Fernando Couto" \\
                --output-dir "./custom_output"
            
            # Merge with custom base path for assessment folders
            python src/main.py syngenta team_assessment merge-assessment \\
                --member-name "Josiel Nascimento" \\
                --base-path "./data/assessments"
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--member-name",
            type=str,
            required=True,
            help="Full name of the team member (e.g., 'Italo Ortega').",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            required=False,
            default=None,
            help="Directory to save merged assessment output. Defaults to ./output/merged_assessments",
        )
        parser.add_argument(
            "--base-path",
            type=str,
            required=False,
            default=None,
            help="Base path for assessment period folders. Defaults to ./output",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        """Execute the merge assessment command.

        Args:
            args: Parsed command-line arguments
        """
        # Load environment variables
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("MergeAssessmentCommand")

        logger.info(f"Starting assessment merge for member: {args.member_name}")

        try:
            # Initialize service
            service = AssessmentMergeService(base_output_path=args.base_path)

            # Prepare output directory
            output_dir = Path(args.output_dir) if args.output_dir else None

            # Execute merge
            merged_data, output_file = service.merge_member_assessments(
                member_name=args.member_name, output_dir=output_dir
            )

            # Display results
            print("=" * 80)
            print(f"MERGE DE AVALIAÇÕES - {args.member_name}")
            print("=" * 80)

            summary = merged_data["summary"]
            print(f"\n✅ Avaliações encontradas: {summary['total_assessments']}")
            print(f"📅 Períodos: {', '.join(summary['periods_covered'])}")

            if summary["overall_trend"].get("trend") != "insufficient_data":
                trend = summary["overall_trend"]
                print(f"📈 Tendência geral: {trend['direction'].upper()}")
                print(f"   Primeiro: {trend['first']:.2f} → Último: {trend['last']:.2f}")
                print(f"   Variação: {trend['change']:+.2f} ({trend['change_percentage']:+.1f}%)")

            print(f"\n💾 Salvo em: {output_file}")
            print("=" * 80)

            logger.info(f"Assessment merge completed successfully. Output: {output_file}")

        except (FileNotFoundError, ValueError) as e:
            # User-friendly validation errors - log without traceback
            logger.error(str(e))
            print(f"\n❌ Error: {e}")
            import sys

            sys.exit(1)
