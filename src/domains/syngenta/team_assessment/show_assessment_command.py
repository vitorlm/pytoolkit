"""Command to display merged assessment data for a team member."""

from argparse import ArgumentParser, Namespace
from pathlib import Path

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

from .services.assessment_display_service import AssessmentDisplayService


class ShowAssessmentCommand(BaseCommand):
    """Command to display merged assessment data for a team member."""

    @staticmethod
    def get_name() -> str:
        return "show-assessment"

    @staticmethod
    def get_description() -> str:
        return "Display merged assessment data for a team member with formatted output."

    @staticmethod
    def get_help() -> str:
        return """
        Display merged assessment data for a team member with formatted output.
        
        This command loads previously merged assessment data and displays it in a
        user-friendly format showing:
        - Assessment details for each period
        - Overall and category averages
        - Self-evaluation status
        - Individual evaluator scores
        - Evolution trends between periods
        - Categories that improved, declined, or remained stable
        
        Examples:
            # Display merged assessment for a member
            python src/main.py syngenta team_assessment show-assessment --member-name "Italo Ortega"
            
            # Display with custom merged data directory
            python src/main.py syngenta team_assessment show-assessment \\
                --member-name "Fernando Couto" \\
                --merged-dir "./custom_output/merged"
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
            "--merged-dir",
            type=str,
            required=False,
            default=None,
            help="Directory containing merged assessment files. Defaults to ./output/merged_assessments",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        """Execute the show assessment command.

        Args:
            args: Parsed command-line arguments
        """
        # Load environment variables
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("ShowAssessmentCommand")

        logger.info(f"Displaying merged assessment for member: {args.member_name}")

        try:
            # Initialize service
            service = AssessmentDisplayService()

            # Prepare merged directory
            merged_dir = Path(args.merged_dir) if args.merged_dir else None

            # Display assessment
            output = service.display_merged_assessment(member_name=args.member_name, merged_dir=merged_dir)

            print(output)

            logger.info(f"Assessment display completed successfully for {args.member_name}")

        except FileNotFoundError as fnfe:
            # User-friendly validation error - log without traceback
            logger.error(f"File not found: {fnfe}")
            print(f"\n❌ Erro: Arquivo de merge não encontrado para {args.member_name}")
            print("💡 Execute primeiro o comando merge-assessment para gerar os dados.")
            import sys

            sys.exit(1)
        except ValueError as ve:
            # User-friendly validation error - log without traceback
            logger.error(str(ve))
            print(f"\n❌ Error: {ve}")
            import sys

            sys.exit(1)
