import argparse
import os
from typing import Dict, Tuple, Union

import pandas as pd

from log_config import log_manager
from utils.excel_manager import ExcelManager
from utils.json_manager import JSONManager

from .config import Config
from .feedback_specialist import FeedbackSpecialist

# Constants
OUTLIER_THRESHOLD = 1.5

# Configure logger
logger = log_manager.get_logger(
    module_name=os.path.splitext(os.path.basename(__file__))[0]
)

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


def get_arguments(parser: argparse.ArgumentParser):
    """Registers arguments for the competencies matrix command."""
    parser.add_argument("--folder", type=str, help="Directory with Excel files")
    parser.add_argument("--output", type=str, help="JSON output file")


def _validate_and_strip(
    field: Union[str, float, int], allow_digits: bool = False
) -> Union[str, int, None]:
    """Validates and processes a field, stripping whitespace and handling NaN values."""
    if pd.isna(field) or field == "":
        return None
    if isinstance(field, str):
        field = field.strip()
        if allow_digits and field.isdigit():
            return int(field)
    return field


def _extract_and_validate_row(row: pd.Series) -> Tuple[Union[str, None], ...]:
    """Extracts and validates the values from a row."""
    criteria, indicator, level, evidence = row.iloc[:4]
    return (
        _validate_and_strip(criteria) if criteria != "Criteria" else None,
        _validate_and_strip(indicator) if indicator != "Indicator" else None,
        _validate_and_strip(level, allow_digits=True) if level != "Level" else None,
        _validate_and_strip(evidence) if evidence != "Evidence" else None,
    )


def process_files(folder_path: str) -> Dict[str, Dict]:
    """Loads and processes Excel files to extract competency data."""
    logger.info("Loading Excel files...")
    excel_files = ExcelManager.load_multiple_excel_files(folder_path)

    peer_feedback_data = {}

    for file_name, excel_data in excel_files:
        logger.info(f"Processing file: {file_name}")
        try:
            evaluator = file_name.split(" - ")[1].replace(".xlsx", "").strip()
            logger.info(f"Evaluator identified: {evaluator}")

            for sheet_name in excel_data.sheet_names[1:]:
                df = excel_data.parse(sheet_name)
                evaluating = sheet_name.strip()
                logger.info(f"Evaluating member: {evaluating}")

                if evaluating not in peer_feedback_data:
                    peer_feedback_data[evaluating] = {}

                competency_matrix = peer_feedback_data[evaluating].setdefault(
                    evaluator, {}
                )

                last_criteria = None

                for _, row in df.iterrows():
                    criteria, indicator, level, evidence = _extract_and_validate_row(
                        row
                    )
                    if criteria is None and last_criteria is not None:
                        criteria = last_criteria
                    elif criteria:
                        last_criteria = criteria

                    if criteria and indicator and level is not None:
                        competency_matrix.setdefault(criteria, []).append(
                            {
                                "indicator": indicator,
                                "level": level,
                                "evidence": evidence,
                            }
                        )
        except Exception as e:
            logger.error(f"Error processing file '{file_name}': {e}", exc_info=True)

    return peer_feedback_data


def calculate_team_statistics(competency_matrix: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Calculates team-level statistics for benchmarking.

    Args:
        competency_matrix (Dict[str, Dict]): JSON structure containing team member evaluations.

    Returns:
        Dict[str, Dict]: Team statistics including averages, highest, and lowest levels.
    """
    team_stats = {
        "overall_levels": [],
        "criteria_stats": {},
    }

    for member_data in competency_matrix.values():
        for _, criteria in member_data.items():
            for criterion, indicators in criteria.items():
                if criterion not in team_stats["criteria_stats"]:
                    team_stats["criteria_stats"][criterion] = {
                        "levels": [],
                        "indicator_stats": {},
                    }
                for indicator in indicators:
                    indicator_name = indicator.get("indicator")
                    if indicator_name not in team_stats["criteria_stats"][
                        criterion
                    ].get("indicator_stats"):
                        team_stats["criteria_stats"][criterion]["indicator_stats"][
                            indicator_name
                        ] = {
                            "levels": [],
                            "average": 0.0,
                            "highest": 0,
                            "lowest": 0,
                        }
                    level = indicator.get("level")
                    if level is not None:
                        team_stats["criteria_stats"][criterion]["indicator_stats"][
                            indicator_name
                        ]["levels"].append(level)
                        team_stats["criteria_stats"][criterion]["levels"].append(level)
                        team_stats["overall_levels"].append(level)

    # Calculate team-level statistics
    team_criteria_stats = team_stats["criteria_stats"]
    for criterion, stats in team_criteria_stats.items():
        levels = stats["levels"]
        if levels:
            for indicator, indicator_stats in stats["indicator_stats"].items():
                indicator_levels = indicator_stats["levels"]
                if indicator_levels:
                    indicator_stats["average"] = round(
                        sum(indicator_levels) / len(indicator_levels), 2
                    )
                    indicator_stats["highest"] = max(levels)
                    indicator_stats["lowest"] = min(levels)
                    indicator_stats.pop("levels")

            team_criteria_stats[criterion]["average"] = round(
                sum(levels) / len(levels), 2
            )
            team_criteria_stats[criterion]["highest"] = max(levels)
            team_criteria_stats[criterion]["lowest"] = min(levels)
            team_criteria_stats[criterion].pop("levels")

    team_stats["average_level"] = (
        round(sum(team_stats["overall_levels"]) / len(team_stats["overall_levels"]), 2)
        if team_stats["overall_levels"]
        else 0
    )
    team_stats["highest_level"] = (
        max(team_stats["overall_levels"]) if team_stats["overall_levels"] else 0
    )
    team_stats["lowest_level"] = (
        min(team_stats["overall_levels"]) if team_stats["overall_levels"] else 0
    )
    team_stats.pop("overall_levels")
    team_stats["criteria_stats"] = team_criteria_stats

    return team_stats


def calculate_individual_statistics(
    competency_matrix: Dict[str, Dict], team_stats: Dict[str, Dict]
) -> Dict[str, Dict]:
    """
    Calculates individual statistics for each team member and includes comparisons with team benchmarks.

    Args:
        competency_matrix (Dict[str, Dict]): JSON structure containing team member evaluations.
        team_stats (Dict[str, Dict]): Team-level statistics for benchmarking.

    Returns:
        Dict[str, Dict]: Individual statistics for each member including averages, strengths, areas for improvement,
                         and comparisons with team benchmarks.
    """
    analysis_results = {}

    for member_name, member_data in competency_matrix.items():
        member_stats = {}
        criterion_stats = {}
        overall_levels = []

        for _, criteria in member_data.items():
            for criterion, indicators in criteria.items():
                if criterion not in criterion_stats:
                    criterion_stats[criterion] = {
                        "levels": [],
                        "average": 0.0,
                        "highest": 0,
                        "lowest": 0,
                        "team_comparison": {},
                        "indicator_stats": {},
                    }

                for indicator in indicators:
                    indicator_name = indicator.get("indicator")
                    if indicator_name not in criterion_stats[criterion].get(
                        "indicator_stats"
                    ):
                        criterion_stats[criterion]["indicator_stats"][
                            indicator_name
                        ] = {
                            "levels": [],
                            "average": 0.0,
                            "highest": 0,
                            "lowest": 0,
                            "team_comparison": {},
                        }
                    level = indicator.get("level")
                    if level is not None:
                        criterion_stats[criterion]["levels"].append(level)
                        criterion_stats[criterion]["indicator_stats"][indicator_name][
                            "levels"
                        ].append(level)
                        overall_levels.append(level)

        # Calculate statistics for each criterion for the member
        for criterion, stats in criterion_stats.items():
            levels = stats["levels"]
            if levels:
                stats["average"] = round(sum(levels) / len(levels), 2)
                stats["highest"] = max(levels)
                stats["lowest"] = min(levels)
                stats.pop("levels")

                # Add team comparison
                team_criteria = team_stats["criteria_stats"].get(criterion, {})
                stats["team_comparison"] = {
                    "average": round(
                        team_criteria.get("average", 0) - stats["average"], 2
                    ),
                    "highest": round(
                        team_criteria.get("highest", 0) - stats["highest"], 2
                    ),
                    "lowest": round(
                        team_criteria.get("lowest", 0) - stats["lowest"], 2
                    ),
                }

                for indicator, indicator_stats in stats["indicator_stats"].items():
                    indicator_levels = indicator_stats["levels"]
                    if indicator_levels:
                        indicator_stats["average"] = round(
                            sum(indicator_levels) / len(indicator_levels), 2
                        )
                        indicator_stats["highest"] = max(indicator_levels)
                        indicator_stats["lowest"] = min(indicator_levels)
                        indicator_stats.pop("levels")

                        # Add team comparison
                        team_indicator = (
                            team_stats["criteria_stats"]
                            .get(criterion, {})
                            .get("indicator_stats", {})
                            .get(indicator, {})
                        )
                        indicator_stats["team_comparison"] = {
                            "average": round(
                                team_indicator.get("average", 0)
                                - indicator_stats["average"],
                                2,
                            ),
                            "highest": round(
                                team_indicator.get("highest", 0)
                                - indicator_stats["highest"],
                                2,
                            ),
                            "lowest": round(
                                team_indicator.get("lowest", 0)
                                - indicator_stats["lowest"],
                                2,
                            ),
                        }

        # Overall stats for the member
        if overall_levels:
            member_stats["average_level"] = round(
                sum(overall_levels) / len(overall_levels), 2
            )
            member_stats["highest_level"] = max(overall_levels)
            member_stats["lowest_level"] = min(overall_levels)
            member_stats["team_comparison"] = {
                "average": round(
                    team_stats["average_level"] - member_stats["average_level"], 2
                ),
                "highest": round(
                    team_stats["highest_level"] - member_stats["highest_level"], 2
                ),
                "lowest": round(
                    team_stats["lowest_level"] - member_stats["lowest_level"], 2
                ),
            }

        member_stats["criteria_statistics"] = criterion_stats
        analysis_results[member_name] = member_stats

    return analysis_results


def detect_outliers(stats: Dict, threshold: float = OUTLIER_THRESHOLD) -> Dict:
    """Detects outliers in team statistics based on the standard deviation."""
    averages = [member["average_level"] for member in stats.values()]
    mean = sum(averages) / len(averages)
    deviations = [(avg - mean) ** 2 for avg in averages]
    std_dev = (sum(deviations) / len(deviations)) ** 0.5

    return {
        member_name: member
        for member_name, member in stats.items()
        if abs(member["average_level"] - mean) > threshold * std_dev
    }


def strengths_opportunities(stats: Dict) -> Dict:
    """Identifies strengths and opportunities for each member compared to team statistics."""
    insights = {}
    for member_name, member in stats.items():
        strengths = []
        opportunities = []
        for criteria, data in member["criteria_statistics"].items():
            if data["average"] > data["team_comparison"]["average"]:
                strengths.append(criteria)
            else:
                opportunities.append(criteria)
        insights[member_name] = {"strengths": strengths, "opportunities": opportunities}
    return insights


def main(args: argparse.Namespace):
    """Main function to process and analyze the competencies matrix."""
    folder_path = args.folder or "competencies"
    output_path = args.output or "team_evaluations.json"

    try:
        logger.info("Starting competencies matrix processing...")
        competency_matrix = process_files(folder_path)

        team_stats = calculate_team_statistics(competency_matrix)
        individual_stats = calculate_individual_statistics(
            competency_matrix, team_stats
        )

        outliers = detect_outliers(individual_stats)
        insights = strengths_opportunities(individual_stats)

        results = {
            "team_statistics": team_stats,
            "individual_statistics": individual_stats,
            "outliers": outliers,
            "insights": insights,
        }

        logger.info(f"Saving results to {output_path}")
        JSONManager.write_json(results, output_path)
        logger.info(
            f"Processing completed successfully. Results saved at {output_path}."
        )
    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process competencies matrix")
    get_arguments(parser)
    main(parser.parse_args())
