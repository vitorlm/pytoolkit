import argparse

from utils.logging_manager import LogManager
from utils.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.string_utils import StringUtils
from utils.base_command import BaseCommand

from .processors.competency_processor import CompetencyProcessor
from .processors.task_processor import TaskProcessor
from .processors.health_check_processor import HealthCheckProcessor
from .services.competency_analyzer import CompetencyAnalyzer
from .services.feedback_specialist import FeedbackSpecialist
from .core.member import Member

# Configure logger
logger = LogManager.get_instance().get_logger("ProcessMatrix")


class ProcessMatrixCommand(BaseCommand):
    """
    Command to process and analyze the competencies matrix.
    """

    @staticmethod
    def get_arguments(parser: argparse.ArgumentParser) -> None:
        """
        Registers arguments for the competencies matrix command.

        Args:
            parser (argparse.ArgumentParser): The argument parser to configure.
        """
        parser.add_argument(
            "--feedbackFolder", type=str, help="Directory with Excel feedback files"
        )
        parser.add_argument(
            "--planningFolder", type=str, help="Directory with Excel planning files"
        )
        parser.add_argument(
            "--healthCheckFolder", type=str, help="Directory with Excel health check files"
        )
        parser.add_argument("--output", type=str, help="JSON output file")

    @staticmethod
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
            f"Starting assessment evaluation using folders: "
            f"{feedback_folder_path}, {planning_folder_path}, {health_check_folder_path}"
        )

        # Validate folders
        for folder in [feedback_folder_path, planning_folder_path, health_check_folder_path]:
            FileManager.validate_folder(folder)

        # Initialize processors and services
        task_processor = TaskProcessor()
        competency_processor = CompetencyProcessor()
        health_check_processor = HealthCheckProcessor()
        feedback_specialist = FeedbackSpecialist()
        competency_analyzer = CompetencyAnalyzer(feedback_specialist)

        members = {}
        try:
            ProcessMatrixCommand._process_tasks(planning_folder_path, task_processor, members)
            ProcessMatrixCommand._process_health_checks(
                health_check_folder_path, health_check_processor, members
            )
            competency_matrix = ProcessMatrixCommand._process_feedback(
                feedback_folder_path, competency_processor, members
            )
            ProcessMatrixCommand._update_members_with_feedback(members, competency_matrix)

            team_stats, members_stats = competency_analyzer.analyze(competency_matrix)
            ProcessMatrixCommand._update_members_with_stats(members, members_stats)

            # Prepare output
            output_data = ProcessMatrixCommand._prepare_output(members, team_stats)
            JSONManager.write_json(output_data, output_path)
            logger.info(f"Competency matrix successfully saved to {output_path}")

        except Exception as e:
            logger.error(f"Unexpected error during processing: {e}", exc_info=True)
            raise

    @staticmethod
    def _process_tasks(planning_folder_path, task_processor, members):
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
                members[member_name].tasks = list(member_tasks)
        logger.info("Task map successfully processed.")

    @staticmethod
    def _process_health_checks(health_check_folder_path, health_check_processor, members):
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

    @staticmethod
    def _process_feedback(feedback_folder_path, competency_processor, members):
        logger.info(f"Processing feedback folder: {feedback_folder_path}")
        competency_matrix = competency_processor.process_folder(feedback_folder_path)
        logger.info("Competency matrix successfully processed.")
        return competency_matrix

    @staticmethod
    def _update_members_with_feedback(members, competency_matrix):
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

    @staticmethod
    def _update_members_with_stats(members, members_stats):
        for member_name, member_stats in members_stats.items():
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

    @staticmethod
    def _prepare_output(members, team_stats):
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
        return {"members": members_data, "team": team_stats}
