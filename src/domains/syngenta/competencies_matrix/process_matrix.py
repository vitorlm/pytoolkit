import logging

import pandas as pd

from utils.excel import load_excel_files
from utils.json import write_json
from utils.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


def get_arguments(parser):
    """
    Registers arguments for the competencies matrix command.

    Args:
        parser (argparse.ArgumentParser): The argument parser to configure.
    """
    parser.add_argument("--folder", type=str, help="Directory with Excel files")
    parser.add_argument("--output", type=str, help="JSON output file")


def _extract_and_validate_row(row):
    """
    Extracts and validates the values from a row.

    Args:
        row (pd.Series): A row from a DataFrame to extract and validate.

    Returns:
        Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
            A tuple containing validated values for criteria, indicator, level, and evidence.
    """
    criteria, indicator, level, evidence = row.iloc[:4]

    def validate_field(field, allow_digits=False):
        if pd.isna(field) or field == "":
            return None
        if isinstance(field, str):
            field = field.strip()
            if allow_digits and field.isdigit():
                return int(field)
        return field

    criteria = validate_field(criteria)
    indicator = validate_field(indicator)
    level = validate_field(level, allow_digits=True)
    evidence = validate_field(evidence)

    if level is None or criteria is None or indicator is None:
        return None, None, None, None

    return criteria, indicator, level, evidence


def _process_file(file_name, excel_data, team_evaluations):
    """
    Processes an Excel file and updates the team's evaluations dictionary.

    Args:
        file_name (str): The name of the file being processed.
        excel_data (pd.ExcelFile): The Excel object for the file.
        team_evaluations (dict): Dictionary containing team evaluations.
    """
    evaluator = file_name.split(" - ")[1].replace(".xlsx", "").strip()

    for sheet_name in excel_data.sheet_names[1:]:
        df = excel_data.parse(sheet_name)
        evaluated_member = sheet_name.strip()

        team_evaluations.setdefault(
            evaluated_member, {"evaluations": {}, "summary": {}}
        )
        team_evaluations[evaluated_member]["evaluations"].setdefault(evaluator, {})

        last_criteria = None
        start_processing = False

        for _, row in df.iterrows():
            if not start_processing:
                if {"Criteria", "Indicator", "Level", "Evidences"}.issubset(
                    row._values
                ):
                    start_processing = True
                continue

            criteria, indicator, level, evidence = _extract_and_validate_row(row)
            if criteria is None and last_criteria is not None:
                criteria = last_criteria
            elif criteria:
                last_criteria = criteria

            if criteria and indicator and level:
                evaluations = team_evaluations[evaluated_member]["evaluations"]
                evaluations[evaluator].setdefault(criteria, []).append(
                    {
                        "indicator": indicator,
                        "level": level,
                        "evidence": evidence,
                    }
                )


def main(args):
    """
    Main function to process and consolidate the competencies matrix.

    Args:
        args (argparse.Namespace): Arguments provided to the script.
    """
    folder_path = args.folder if args.folder else "competencies"
    output_path = args.output if args.output else "consolidated_team_evaluations.json"

    logger.info("Starting competencies matrix processing")
    team_evaluations = {}

    excel_files = load_excel_files(folder_path)
    for file_name, excel_data in excel_files:
        _process_file(file_name, excel_data, team_evaluations)

    write_json(team_evaluations, output_path)
    logger.info("Processing completed successfully")
