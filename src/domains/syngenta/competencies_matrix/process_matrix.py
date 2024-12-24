import argparse
import os

from log_config import log_manager
from utils.json_manager import JSONManager

from .competency_processor import CompetencyProcessor

# Configure logger
logger = log_manager.get_logger(
    module_name=os.path.splitext(os.path.basename(__file__))[0]
)


def get_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Registers arguments for the competencies matrix command.

    Args:
        parser (argparse.ArgumentParser): The argument parser to configure.
    """
    parser.add_argument("--folder", type=str, help="Directory with Excel files")
    parser.add_argument("--output", type=str, help="JSON output file")


def main(args: argparse.Namespace) -> None:
    """
    Main function to process and analyze the competencies matrix.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.

    Raises:
        FileNotFoundError: If the specified folder does not exist.
        ValueError: If an Excel file contains malformed data.
    """
    folder_path = args.folder or "competencies"
    output_path = args.output or "competency_matrix.json"

    if not os.path.exists(folder_path):
        logger.error(f"Specified folder does not exist: {folder_path}")
        raise FileNotFoundError(f"Specified folder does not exist: {folder_path}")

    logger.info(f"Starting competencies matrix processing from folder: {folder_path}")

    competency_matrix_processor = CompetencyProcessor()

    try:
        competency_matrix = competency_matrix_processor.process_excel_files(folder_path)
    except FileNotFoundError as e:
        logger.error(f"Missing file error during processing: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"Malformed data in one or more files: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}", exc_info=True)
        raise

    try:
        JSONManager.write_json(competency_matrix, output_path)
        logger.info(f"Competency matrix successfully saved to {output_path}")
    except Exception as e:
        logger.error(f"Error writing output file: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process competencies matrix")
    get_arguments(parser)
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        logger.critical(f"Critical failure in process_matrix: {e}", exc_info=True)
