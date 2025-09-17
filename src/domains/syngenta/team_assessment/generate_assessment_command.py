from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from log_config import LogManager
from .assessment_generator import AssessmentGenerator

# Configure logger
logger = LogManager.get_instance().get_logger("AssessmentGeneratorCommand")


class GenerateAssessmentCommand(BaseCommand):
    """
    Command to generate assessment reports by aggregating competency matrix,
    health check, and planning data.
    """

    @staticmethod
    def get_name() -> str:
        return "generate_assessment"

    @staticmethod
    def get_description() -> str:
        return (
            "Generates assessment reports by aggregating feedback, planning, "
            "and health check data into detailed team and member reports."
        )

    @staticmethod
    def get_help() -> str:
        return (
            "Use this command to process competency matrix, planning files, and health checks. "
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
            "--planningFolder",
            type=str,
            required=False,
            default=None,
            help="Path to the directory containing planning Excel files.",
        )
        parser.add_argument(
            "--healthCheckFolder",
            type=str,
            required=False,
            default=None,
            help="Path to the directory containing health check Excel files.",
        )
        parser.add_argument(
            "--output",
            type=str,
            required=True,
            help="Path to save the generated assessment report as a JSON file.",
        )
        parser.add_argument(
            "--ignoredMembers",
            type=str,
            required=False,
            help="Path to the file containing the list of members to ignore in the assessment.",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        """
        Main function to generate the assessment report.

        Args:
            args (Namespace): Parsed command-line arguments.

        Raises:
            FileNotFoundError: If the input folders are invalid or inaccessible.
            ValueError: If the input data is malformed.
            Exception: For other unexpected errors.
        """
        try:
            logger.info(
                "Starting the assessment generation process with the following inputs:"
                f"\nCompetency Matrix folder: {args.competencyMatrixFile}"
                f"\nFeedback Folder: {args.feedbackFolder}"
                f"\nPlanning Folder: {args.planningFolder}"
                f"\nHealth Check Folder: {args.healthCheckFolder}"
                f"\nOutput File: {args.output}"
                f"\nIgnored Members File: {args.ignoredMembers}"
            )

            processor = AssessmentGenerator(
                competency_matrix_file=args.competencyMatrixFile,
                feedback_folder=args.feedbackFolder,
                planning_folder=args.planningFolder,
                health_check_folder=args.healthCheckFolder,
                output_path=args.output,
                ignored_member_list=args.ignoredMembers,
            )

            processor.run()
            logger.info(f"Assessment report successfully generated and saved to {args.output}")

        except FileNotFoundError as fnfe:
            logger.error(f"File not found: {fnfe}", exc_info=True)
            raise
        except ValueError as ve:
            logger.error(f"Invalid data: {ve}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise
