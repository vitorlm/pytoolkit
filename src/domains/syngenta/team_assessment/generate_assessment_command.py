from argparse import ArgumentParser, Namespace

from log_config import LogManager
from utils.command.base_command import BaseCommand

from .assessment_generator import AssessmentGenerator

# Configure logger
logger = LogManager.get_instance().get_logger("AssessmentGeneratorCommand")


class GenerateAssessmentCommand(BaseCommand):
    """Command to generate assessment reports by aggregating competency matrix,
    health check, and planning data.
    """

    @staticmethod
    def get_name() -> str:
        return "generate_assessment"

    @staticmethod
    def get_description() -> str:
        return (
            "Generates assessment reports by aggregating feedback and planning "
            "data into detailed team and member reports."
        )

    @staticmethod
    def get_help() -> str:
        return (
            "Use this command to process competency matrix and planning files. "
            "Outputs a comprehensive JSON report for team members and overall team performance."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--competencyMatrixFile",
            type=str,
            required=True,
            help="Path to the competency matrix Excel file.",
        )
        parser.add_argument(
            "--feedbackFolder",
            type=str,
            required=True,
            help="Path to the directory containing feedback Excel files.",
        )
        parser.add_argument(
            "--planningFile",
            type=str,
            required=False,
            default=None,
            help="Path to the planning Excel file (.xlsm or .xlsx) containing task allocations.",
        )
        parser.add_argument(
            "--outputFolder",
            type=str,
            required=True,
            help="Path to the folder where assessment reports will be saved (JSON and charts).",
        )
        parser.add_argument(
            "--ignoredMembers",
            type=str,
            required=False,
            help="Path to the JSON file containing the list of member names to ignore in the assessment.",
        )
        parser.add_argument(
            "--disableHistorical",
            action="store_true",
            default=False,
            help="Disable historical period discovery and comparison (enabled by default).",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        """Main function to generate the assessment report.

        Args:
            args (Namespace): Parsed command-line arguments.
        """
        # Historical processing is enabled by default, unless explicitly disabled
        enable_historical = not args.disableHistorical

        logger.info("Starting the assessment generation process with the following inputs:")
        logger.info(f"  Competency Matrix File: {args.competencyMatrixFile}")
        logger.info(f"  Feedback Folder: {args.feedbackFolder}")
        logger.info(f"  Planning File: {args.planningFile or 'Not provided'}")
        logger.info(f"  Output Folder: {args.outputFolder}")
        logger.info(f"  Ignored Members File: {args.ignoredMembers or 'Not provided'}")
        logger.info(f"  Historical Processing: {'Enabled' if enable_historical else 'Disabled'}")

        try:
            processor = AssessmentGenerator(
                competency_matrix_file=args.competencyMatrixFile,
                feedback_folder=args.feedbackFolder,
                planning_file=args.planningFile,
                output_path=args.outputFolder,
                ignored_member_list=args.ignoredMembers,
                enable_historical=enable_historical,
            )

            processor.run()
            logger.info(f"Assessment report successfully generated and saved to {args.outputFolder}")

        except (FileNotFoundError, ValueError) as e:
            # User-friendly validation errors - log without traceback
            logger.error(str(e))
            import sys

            sys.exit(1)
