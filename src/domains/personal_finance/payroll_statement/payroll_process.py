import argparse
import os
from .payroll_statement_processor import PayrollStatementProcessor
from utils.json_manager import JSONManager
from log_config import log_manager
from utils.file_manager import FileManager

# Configure logger
logger = log_manager.get_logger(module_name=os.path.splitext(os.path.basename(__file__))[0])


def get_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--input",
        type=str,
        required=False,
        help="Path to a single PDF file or a folder containing PDF files",
    )
    parser.add_argument("--output", type=str, required=True, help="Path to the output JSON file")


def main(args: argparse.Namespace) -> None:
    """
    Main function to process payroll statements and save the extracted data as JSON.
    """

    processor = PayrollStatementProcessor()
    try:
        if FileManager.is_folder(args.input):
            data = processor.process_folder(args.input)
        else:
            data = processor.process_pdf(args.input)

        JSONManager.write_json(data, args.output)
        log_manager.get_logger(module_name="payroll_statement_main").info(
            f"Payroll statement data saved to {args.output}"
        )
    except Exception as e:
        log_manager.get_logger(module_name="payroll_statement_main").error(
            f"An error occurred: {e}", exc_info=True
        )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process competencies matrix")
    get_arguments(parser)
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        logger.critical(f"Critical failure in process_matrix: {e}", exc_info=True)
