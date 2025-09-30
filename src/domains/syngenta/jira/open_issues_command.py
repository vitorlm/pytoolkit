"""
JIRA Open Issues Command

This command fetches all currently open issues from a JIRA project without date filtering.
It's designed to get a snapshot of all active work items at the current moment.

FUNCTIONALITY:
- Fetch all issues that are currently open (not Done/Resolved)
- Filter by issue types (configurable parameter)
- Filter by team/squad (optional)
- Filter by specific status categories
- Export results to JSON file

USAGE EXAMPLES:

1. Get all open bugs:
   python src/main.py syngenta jira open-issues --project-key "CWS" --issue-types "Bug"

2. Get all open issues for a specific team:
   python src/main.py syngenta jira open-issues --project-key "CWS"
   --issue-types "Story,Task,Bug" --team "Catalog"

2b. Get all open issues for multiple teams:
   python src/main.py syngenta jira open-issues --project-key "CWS"
   --issue-types "Story,Task,Bug" --teams "Catalog,Platform"

3. Get all open issues with custom status:
   python src/main.py syngenta jira open-issues --project-key "CWS"
   --issue-types "Bug,Support" --status-categories "To Do,In Progress"

4. Export to file:
   python src/main.py syngenta jira open-issues --project-key "CWS"
   --issue-types "Bug" --output-file "open_bugs.json"

ISSUE TYPES (examples):
- Bug, Support, Story, Task, Epic, Technical Debt, Improvement

STATUS CATEGORIES:
- To Do: Issues not started yet
- In Progress: Issues currently being worked on
- Done: Completed issues (excluded by default)
"""

from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.open_issues_service import OpenIssuesService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class OpenIssuesCommand(BaseCommand):
    """Command to fetch all currently open issues from JIRA."""

    @staticmethod
    def get_name() -> str:
        return "open-issues"

    @staticmethod
    def get_description() -> str:
        return "Fetch all currently open issues from a JIRA project."

    @staticmethod
    def get_help() -> str:
        return (
            "This command fetches all currently open issues from a JIRA project "
            "without date filtering. It provides a snapshot of all active work items."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--project-key",
            type=str,
            required=True,
            help="The JIRA project key to analyze (e.g., 'CWS', 'PROJ').",
        )
        parser.add_argument(
            "--issue-types",
            type=str,
            required=True,
            help="Comma-separated list of issue types to fetch (e.g., 'Bug', 'Story,Task,Bug').",
        )
        parser.add_argument(
            "--team",
            "--teams",
            dest="teams",
            action="append",
            required=False,
            help=(
                "Filter by one or more teams using Squad[Dropdown] field. "
                "You can repeat --team/--teams or pass a comma-separated list (e.g., 'Catalog,Platform')."
            ),
        )
        parser.add_argument(
            "--status-categories",
            type=str,
            required=False,
            default="To Do,In Progress",
            help=(
                "Comma-separated list of status categories to include "
                "(default: 'To Do,In Progress'). Options: 'To Do', 'In Progress', 'Done'."
            ),
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the results in JSON format.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed issue information.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute open issues retrieval.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("OpenIssuesCommand")

        try:
            # Parse issue types
            issue_types = [t.strip() for t in args.issue_types.split(",")]

            # Parse status categories
            status_categories = [s.strip() for s in args.status_categories.split(",")]

            # Initialize service
            service = OpenIssuesService()

            # Parse teams (repeatable flag and comma-separated)
            raw_teams = getattr(args, "teams", []) or []
            teams = [
                t.strip() for entry in raw_teams for t in entry.split(",") if t.strip()
            ] or None

            # Fetch open issues
            result = service.fetch_open_issues(
                project_key=args.project_key,
                issue_types=issue_types,
                teams=teams,
                status_categories=status_categories,
                verbose=args.verbose,
                output_file=args.output_file,
            )

            if result:
                logger.info("Open issues retrieval completed successfully")

                # Print summary
                print("\n" + "=" * 50)
                print("OPEN ISSUES SUMMARY")
                print("=" * 50)
                print(f"Project: {args.project_key}")
                print(f"Issue Types: {', '.join(issue_types)}")
                print(f"Status Categories: {', '.join(status_categories)}")
                meta_team = result.get("team") or result.get(
                    "analysis_metadata", {}
                ).get("team")
                if meta_team:
                    print(f"Teams: {meta_team}")

                # Print metrics
                total_issues = result.get("total_issues", 0)
                print(f"\nTotal Open Issues: {total_issues}")

                # Print breakdown by status
                status_breakdown = result.get("status_breakdown", {})
                if status_breakdown:
                    print("\nBreakdown by Status:")
                    for status, count in status_breakdown.items():
                        print(f"  {status}: {count}")

                # Print breakdown by issue type
                type_breakdown = result.get("type_breakdown", {})
                if type_breakdown:
                    print("\nBreakdown by Issue Type:")
                    for issue_type, count in type_breakdown.items():
                        print(f"  {issue_type}: {count}")

                # Print breakdown by priority
                priority_breakdown = result.get("priority_breakdown", {})
                if priority_breakdown:
                    print("\nBreakdown by Priority:")
                    for priority, count in priority_breakdown.items():
                        print(f"  {priority}: {count}")

                if args.output_file:
                    print(f"\nDetailed report saved to: {args.output_file}")

                print("=" * 50)
            else:
                logger.error("Open issues retrieval failed")
                exit(1)

        except Exception as e:
            logger.error(f"Failed to execute open issues retrieval: {e}")
            print(f"Error: Failed to execute open issues retrieval: {e}")
            exit(1)
