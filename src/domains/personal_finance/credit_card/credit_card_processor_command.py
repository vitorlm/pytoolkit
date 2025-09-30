from argparse import ArgumentParser, Namespace
from domains.personal_finance.credit_card.credit_card_processor import (
    CreditCardProcessor,
)
from utils.command.base_command import BaseCommand
from utils.logging.logging_manager import LogManager


# Configure logger
logger = LogManager.get_instance().get_logger("CreditCardProcessorCommand")


class CreditCardProcessorCommand(BaseCommand):
    """
    Command to process credit card statements and classify expenses.
    """

    @staticmethod
    def get_name() -> str:
        return "process_credit_card"

    @staticmethod
    def get_description() -> str:
        return (
            "Processes credit card statement CSV file and classifies expenses "
            "based on transaction descriptions."
        )

    @staticmethod
    def get_help() -> str:
        return (
            "Use this command to analyze a credit card statement CSV file and "
            "generate a categorized expense report in JSON format."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--input_file",
            type=str,
            required=True,
            help="Path to the credit card statement CSV file.",
        )
        parser.add_argument(
            "--output",
            type=str,
            required=True,
            help="Path to save the categorized expense report as a JSON file.",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        """
        Main function to process the credit card statement and generate categorized report.

        Args:
            args (Namespace): Parsed command-line arguments.

        Raises:
            FileNotFoundError: If the input file is invalid or inaccessible.
            ValueError: If the CSV data is malformed.
            Exception: For other unexpected errors.
        """
        try:
            logger.info(
                f"Starting credit card statement processing with inputs:"
                f"\nInput File: {args.input_file}"
                f"\nOutput File: {args.output}"
            )

            processor = CreditCardProcessor(args.input_file, args.output)
            processor.run()
            logger.info(
                f"Expense report successfully generated and saved to {args.output}"
            )

        except FileNotFoundError as fnfe:
            logger.error(f"File not found: {fnfe}", exc_info=True)
            raise
        except ValueError as ve:
            logger.error(f"Invalid data: {ve}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise
