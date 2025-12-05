from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager

from .weekly_report_service import WeeklyReportService


class WeeklyReportCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "weekly-report"

    @staticmethod
    def get_description() -> str:
        return "Generates a consolidated weekly engineering report from JIRA, LinearB, SonarQube, and CircleCI data."

    @staticmethod
    def get_help() -> str:
        return (
            "This command aggregates all necessary metrics and outputs a markdown report.\n"
            "It consumes existing output files or triggers domain commands if needed.\n"
            "Example usage:\n"
            "python src/main.py report weekly-report --scope tribe --period last-week\n"
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--scope",
            choices=["tribe", "team"],
            required=True,
            help="Report scope: tribe or team",
        )
        parser.add_argument(
            "--period",
            type=str,
            required=True,
            help="Reporting period (e.g., last-week, last-2-weeks)",
        )
        parser.add_argument("--team", type=str, help="Team name (required for team scope)")
        parser.add_argument(
            "--output-dir",
            type=str,
            default="output",
            help="Output directory for report files",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("WeeklyReportCommand")
        try:
            service = WeeklyReportService()
            report_path = service.generate_report(args)
            logger.info(f"Weekly report generated: {report_path}")
        except Exception as e:
            logger.error(f"Failed to generate weekly report: {e}", exc_info=True)
            exit(1)
