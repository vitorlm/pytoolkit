from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from .processors.team_task_processor import TeamTaskProcessor


class EpicsAdherencePlanningCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "epics-adherence-planning"

    @staticmethod
    def get_description() -> str:
        return "Command for planning epics adherence."

    @staticmethod
    def get_help() -> str:
        return "Helps in planning the adherence of epics."

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--planningFolder",
            type=str,
            required=True,
            help="Path to the directory containing planning Excel files.",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        task_processor = TeamTaskProcessor()
        task_processor.process_folder(args.planningFolder)
