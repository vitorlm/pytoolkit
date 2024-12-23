import argparse
import os

import pandas as pd

from log_config import log_manager
from utils.excel import load_excel_files
from utils.json import write_json

from .config import Config
from .feedback_specialist import FeedbackSpecialist

# Configure logger
logger = log_manager.get_logger(os.path.splitext(os.path.basename(__file__))[0])

# Initialize FeedbackSpecialist
feedback_specialist = FeedbackSpecialist(
    host=Config.HOST,
    model=Config.MODEL,
    num_ctx=Config.NUM_CTX,
    temperature=Config.TEMPERATURE,
    num_thread=Config.NUM_THREAD,
    num_keep=Config.NUM_KEEP,
    top_k=Config.TOP_K,
    top_p=Config.TOP_P,
    repeat_penalty=Config.REPEAT_PENALTY,
    stop=Config.STOP,
    num_predict=Config.NUM_PREDICT,
    presence_penalty=Config.PRESENCE_PENALTY,
    frequency_penalty=Config.FREQUENCY_PENALTY,
)


def get_arguments(parser):
    """Registers arguments for the competencies matrix command."""
    parser.add_argument("--folder", type=str, help="Directory with Excel files")
    parser.add_argument("--output", type=str, help="JSON output file")


def _extract_and_validate_row(row):
    """Extracts and validates the values from a row."""
    criteria, indicator, level, evidence = row.iloc[:4]

    def validate_field(field, allow_digits=False):
        if pd.isna(field) or field == "":
            return None
        if isinstance(field, str):
            field = field.strip()
            if allow_digits and field.isdigit():
                return int(field)
        return field

    return (
        validate_field(criteria) if criteria != "Criteria" else None,
        validate_field(indicator) if indicator != "Indicator" else None,
        validate_field(level, allow_digits=True) if level != "Level" else None,
        validate_field(evidence) if evidence != "Evidence" else None,
    )


def process_files(folder_path):
    """Loads and processes Excel files to extract competency data."""
    logger.info("Loading Excel files...")
    excel_files = load_excel_files(folder_path)
    competency_matrix = {}

    for file_name, excel_data in excel_files:
        logger.info(f"Processing file: {file_name}")
        try:
            evaluator = file_name.split(" - ")[1].replace(".xlsx", "").strip()
            logger.info(f"Evaluator identified: {evaluator}")

            for sheet_name in excel_data.sheet_names[1:]:
                df = excel_data.parse(sheet_name)
                member_name = sheet_name.strip()
                logger.info(f"Evaluating member: {member_name}")

                competency_matrix.setdefault(member_name, {})
                last_criteria = None

                for index, row in df.iterrows():
                    criteria, indicator, level, evidence = _extract_and_validate_row(
                        row
                    )
                    if criteria is None and last_criteria is not None:
                        criteria = last_criteria
                    elif criteria:
                        last_criteria = criteria

                    if criteria and indicator and level is not None:
                        competency_matrix[member_name].setdefault(criteria, []).append(
                            {
                                "indicator": indicator,
                                "level": level,
                                "evidence": evidence,
                            }
                        )
        except Exception as e:
            logger.error(f"Error processing file '{file_name}': {e}", exc_info=True)

    return competency_matrix


def main(args):
    """Main function to process and analyze the competencies matrix."""
    folder_path = args.folder or "competencies"
    output_path = args.output or "team_evaluations.json"

    try:
        logger.info("Starting competencies matrix processing...")
        competency_matrix = process_files(folder_path)

        logger.info("Generating feedback and recommendations...")
        results = feedback_specialist.analyze_matrix(competency_matrix)

        logger.info(f"Saving results to {output_path}")
        write_json(results, output_path)
        logger.info(
            f"Processing completed successfully. Results saved at {output_path}."
        )
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process competencies matrix")
    get_arguments(parser)
    main(parser.parse_args())
