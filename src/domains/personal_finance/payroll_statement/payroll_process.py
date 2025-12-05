from argparse import ArgumentParser, Namespace

from log_config import LogManager
from utils.command.base_command import BaseCommand
from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager

from .payroll_statement_processor import PayrollStatementProcessor

# Configure logger
logger = LogManager.get_instance().get_logger("PayrollProcessCommand")


class PayrollProcessCommand(BaseCommand):
    """Command to process payroll statements into JSON format."""

    @staticmethod
    def get_name() -> str:
        return "payroll_process"

    @staticmethod
    def get_description() -> str:
        return "Processes payroll statements and extracts data into a JSON file."

    @staticmethod
    def get_help() -> str:
        return (
            "This command processes payroll statements from PDFs in a folder or single file "
            "and outputs the extracted data in JSON format."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser) -> None:
        parser.add_argument(
            "--input",
            type=str,
            required=False,
            help="Path to a single PDF file or a folder containing payroll PDFs.",
        )
        parser.add_argument(
            "--output",
            type=str,
            required=True,
            help="Path to save the extracted payroll data as JSON.",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        processor = PayrollStatementProcessor()
        try:
            if FileManager.is_folder(args.input):
                logger.debug(f"Processing all files in folder: {args.input}")
                data = processor.process_folder(args.input)
            else:
                logger.debug(f"Processing single file: {args.input}")
                data = processor.process_pdf(args.input)

            JSONManager.write_json(data, args.output)
            logger.info(f"Payroll data successfully saved to {args.output}")
        except Exception as e:
            logger.error(f"An error occurred during payroll processing: {e}", exc_info=True)
            raise
