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
        parser.add_argument(
            "--jira_project",
            type=str,
            required=False,
            help="JIRA project key to load.",
        )
        parser.add_argument(
            "--team_name",
            type=str,
            required=False,
            help="Name of the team.",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        if not args.planningFolder:
            raise ValueError("The --planningFolder argument is required.")
        if args.jira_project and not args.team_name:
            raise ValueError(
                "The --team_name argument is required when --jira_project is provided."
            )
        if args.team_name and not args.jira_project:
            raise ValueError(
                "The --jira_project argument is required when --team_name is provided."
            )
        task_processor = TeamTaskProcessor()
        if args.jira_project and args.team_name:
            task_processor.process_folder(
                args.planningFolder,
                jira_project=args.jira_project,
                team_name=args.team_name,
            )
        else:
            task_processor.process_folder(args.planningFolder)
