from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.jira_processor import JiraProcessor
from utils.command.base_command import BaseCommand


class FillMissingDatesCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "fill-missing-dates"

    @staticmethod
    def get_description() -> str:
        return "Fills missing dates in JIRA issues."

    @staticmethod
    def get_help() -> str:
        return "This command fills in missing dates for JIRA issues based on certain criteria."

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--project", type=str, required=True, help="The JIRA project key.")
        parser.add_argument("--team_name", type=str, required=True, help="The team name.")
        parser.add_argument(
            "--start-date",
            type=str,
            required=False,
            help="The start date for filling missing dates (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--end-date",
            type=str,
            required=False,
            help="The end date for filling missing dates (YYYY-MM-DD).",
        )

    @staticmethod
    def main(args: Namespace):
        project = args.project
        team_name = args.team_name
        start_date = args.start_date
        end_date = args.end_date

        jira_processor = JiraProcessor()

        # Call the function to fill missing dates in JIRA issues
        jira_processor.fill_missing_dates_for_completed_epics(project, team_name, start_date, end_date)
