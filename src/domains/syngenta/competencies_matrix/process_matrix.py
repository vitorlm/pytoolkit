import argparse
import os

from log_config import log_manager
from utils.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.string_utils import StringUtils

from .competency_processor import CompetencyProcessor, Indicator
from .task_processor import TaskProcessor, Task
from .health_check_processor import HealthCheckProcessor, MemberHealthCheck
from .competency_analyzer import CompetencyAnalyzer, IndividualStatistics
from .feedback_specialist import FeedbackSpecialist
from typing import Dict, List, Optional

# Configure logger
logger = log_manager.get_logger(module_name=os.path.splitext(os.path.basename(__file__))[0])


class Member:
    """
    Model for validating member-level competency data.

    Attributes:
        member (str): Name of the team member.
        indicators (List[Indicator]): List of competency indicators for this member.
    """

    name: str
    last_name: Optional[str]
    feedback: Optional[Dict[str, Dict[str, List[Indicator]]]] = {}
    tasks: Optional[List[Task]] = []
    health_check: Optional[MemberHealthCheck] = None

    def __init__(
        self,
        name: str,
        last_name: Optional[str],
        feedback: Optional[Dict[str, Dict[str, List[Indicator]]]] = None,
        feedback_stats: Optional[IndividualStatistics] = None,
        tasks: Optional[List[Task]] = None,
        health_check: Optional[MemberHealthCheck] = None,
    ):
        self.name = name
        self.last_name = last_name
        self.feedback = feedback or {}
        self.feedback_stats = feedback_stats
        self.tasks = tasks or []
        self.health_check = health_check


def get_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Registers arguments for the competencies matrix command.

    Args:
        parser (argparse.ArgumentParser): The argument parser to configure.
    """
    parser.add_argument("--feedbackFolder", type=str, help="Directory with Excel feedback files")
    parser.add_argument("--planningFolder", type=str, help="Directory with Excel planning files")
    parser.add_argument(
        "--healthCheckFolder", type=str, help="Directory with Excel health check files"
    )
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
    feedback_folder_path = args.feedbackFolder
    planning_folder_path = args.planningFolder
    health_check_folder_path = args.healthCheckFolder
    output_path = args.output

    logger.info(
        f"Starting assessment evoluation processing using folders: "
        f"{feedback_folder_path} and {planning_folder_path}"
    )

    FileManager.validate_folder(feedback_folder_path)
    FileManager.validate_folder(planning_folder_path)
    FileManager.validate_folder(health_check_folder_path)

    task_processor = TaskProcessor()
    competency_matrix_processor = CompetencyProcessor()
    health_check_processor = HealthCheckProcessor()
    feedback_specialist = FeedbackSpecialist()
    competency_analyzer = CompetencyAnalyzer(feedback_specialist)

    members = {}
    try:
        logger.info(f"Processing planning folder: {planning_folder_path}")
        members_tasks = task_processor.process_folder(planning_folder_path)
        for member_name, member_tasks in members_tasks.items():
            member_name = StringUtils.remove_accents(member_name)
            if member_name not in members:
                members[member_name] = Member(
                    name=member_name,
                    last_name="",
                    feedback={},
                    feedback_stats=None,
                    tasks=list(member_tasks),
                    health_check=[],
                )
            else:
                members[member_name].tasks = list(member_tasks)  # Convert set to list

        logger.info("Task map successfully processed.")

        logger.info(f"Processing health check folder: {health_check_folder_path}")
        health_check_data = health_check_processor.process_folder(health_check_folder_path)

        for member_name, health_check in health_check_data.items():
            member_name = StringUtils.remove_accents(member_name)
            if member_name not in members:
                members[member_name] = Member(
                    name=member_name,
                    last_name="",
                    feedback={},
                    feedback_stats=None,
                    tasks=[],
                    health_check=health_check,
                )
            else:
                members[member_name].health_check = health_check
        logger.info("Health check data successfully processed.")

        logger.info(f"Processing feedback folder: {feedback_folder_path}")
        competency_matrix = competency_matrix_processor.process_folder(feedback_folder_path)
        logger.info("Competency matrix successfully processed.")

        # Update members with competency data
        for evaluatee_name, evaluations in competency_matrix.items():
            name, last_name = evaluatee_name.split(" ", 1)
            name = StringUtils.remove_accents(name)
            last_name = StringUtils.remove_accents(last_name)
            if name not in members:
                members[name] = Member(
                    name=name,
                    last_name=last_name,
                    feedback={},
                    feedback_stats=None,
                    tasks=[],
                    health_check=[],
                )

            for evaluator_name, feedback in evaluations.items():
                evaluator_name = StringUtils.remove_accents(evaluator_name)
                members[name].feedback[evaluator_name] = feedback

        logger.info("Members updated with competency data.")

        team_stats, members_stas = competency_analyzer.analyze(competency_matrix)

        for member_name, member_stats in members_stas.items():
            name, last_name = member_name.split(" ", 1)
            name = StringUtils.remove_accents(name)
            last_name = StringUtils.remove_accents(last_name)
            if name not in members:
                members[name] = Member(
                    name=name,
                    last_name=last_name,
                    feedback={},
                    feedback_stats=None,
                    tasks=[],
                    health_check=[],
                )
            members[name].feedback_stats = member_stats

        # Prepare data for JSON output
        members_data = [
            {
                "name": member.name,
                "last_name": member.last_name,
                "feedback": member.feedback,
                "feedback_stats": member.feedback_stats,
                "tasks": [task.dict() for task in member.tasks],
                "health_check": member.health_check,
            }
            for member in members.values()
        ]

        output_data = {"members": members_data, "team": team_stats}

        JSONManager.write_json(output_data, output_path)
        logger.info(f"Competency matrix successfully saved to {output_path}")

    except FileNotFoundError as e:
        logger.error(f"Missing file error during processing: {e}", exc_info=True)
        raise
    except ValueError as e:
        logger.error(f"Malformed data in one or more files: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error during processing: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process competencies matrix")
    get_arguments(parser)
    args = parser.parse_args()

    try:
        main(args)
    except Exception as e:
        logger.critical(f"Critical failure in process_matrix: {e}", exc_info=True)
