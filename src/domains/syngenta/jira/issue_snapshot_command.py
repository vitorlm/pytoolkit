"""JIRA Issue Snapshot Command

This command fetches a snapshot of issues within a specific time window, with comprehensive
filtering options and the ability to include issue comments.

FUNCTIONALITY:
- Fetch issues within an anchor-based time window (e.g., last 7 days from a specific date)
- Filter by single project and issue type
- Filter by team(s) using Squad[Dropdown] field
- Filter by specific status names OR status categories (mutually exclusive)
- Optionally include issue comments
- Export results to JSON and/or Markdown format

USAGE EXAMPLES:

1. Get bugs from last 7 days (default window):
   python src/main.py syngenta jira issue-snapshot --project-key "CWS" \
   --issue-type "Bug" --end-date "2025-10-27"

2. Get stories from last 14 days with team filter:
   python src/main.py syngenta jira issue-snapshot --project-key "CWS" \
   --issue-type "Story" --end-date "2025-10-27" --window-days 14 --team "Catalog"

3. Get tasks from multiple teams with specific statuses:
   python src/main.py syngenta jira issue-snapshot --project-key "CWS" \
   --issue-type "Task" --end-date "2025-10-27" --window-days 7 \
   --teams "Catalog,Platform" --status "Done,Closed"

4. Get issues with comments and export to JSON:
   python src/main.py syngenta jira issue-snapshot --project-key "CWS" \
   --issue-type "Bug" --end-date "2025-10-27" --comments \
   --output-format json

5. Get issues filtered by status categories:
   python src/main.py syngenta jira issue-snapshot --project-key "CWS" \
   --issue-type "Story" --end-date "2025-10-27" --status-categories "Done,In Progress"

6. Export to both JSON and Markdown:
   python src/main.py syngenta jira issue-snapshot --project-key "CWS" \
   --issue-type "Bug" --end-date "2025-10-27" --output-format "json,md"

Notes:
- --status and --status-categories are mutually exclusive
- Default window is 7 days if not specified
- Default end-date is today if not specified
- Comments are not fetched by default (add --comments flag to enable)
"""

from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.issue_snapshot_service import IssueSnapshotService
from domains.syngenta.jira.summary.jira_summary_manager import JiraSummaryManager
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class IssueSnapshotCommand(BaseCommand):
    """Command to fetch a snapshot of issues within a specific time window."""

    @staticmethod
    def get_name() -> str:
        return "issue-snapshot"

    @staticmethod
    def get_description() -> str:
        return "Fetch a snapshot of issues within a specific time window with comprehensive filtering."

    @staticmethod
    def get_help() -> str:
        return (
            "This command fetches issues within an anchor-based time window with support for "
            "team filtering, status filtering, and optional comment retrieval. "
            "Results can be exported to JSON and/or Markdown format."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        # Required arguments
        parser.add_argument(
            "--project-key",
            type=str,
            required=True,
            help="The JIRA project key to analyze (e.g., 'CWS', 'PROJ'). One project only.",
        )
        parser.add_argument(
            "--issue-type",
            type=str,
            required=True,
            help="Single issue type to fetch (e.g., 'Bug', 'Story', 'Task', 'Epic').",
        )

        # Time window arguments
        parser.add_argument(
            "--end-date",
            type=str,
            required=False,
            help=(
                "Anchor date in YYYY-MM-DD format. Defines the end of the analysis window. "
                "Defaults to today if not specified."
            ),
        )
        parser.add_argument(
            "--window-days",
            type=int,
            required=False,
            default=7,
            help="Window size in days counting backwards from end-date (default: 7).",
        )

        # Team filtering
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

        # Status filtering (mutually exclusive)
        status_group = parser.add_mutually_exclusive_group(required=False)
        status_group.add_argument(
            "--status",
            type=str,
            required=False,
            help=(
                "Comma-separated list of status names to include (e.g., 'Done,Closed'). "
                "Cannot be used with --status-categories."
            ),
        )
        status_group.add_argument(
            "--status-categories",
            type=str,
            required=False,
            help=(
                "Comma-separated list of status categories to include (e.g., 'Done', 'In Progress'). "
                "Cannot be used with --status."
            ),
        )

        # Comments
        parser.add_argument(
            "--comments",
            action="store_true",
            help="Include this flag to fetch comments for each issue. Comments are not fetched by default.",
        )
        parser.add_argument(
            "--text-format",
            type=str,
            choices=["adf", "plaintext"],
            default="adf",
            help=(
                "Format for description and comments: 'adf' (Atlassian Document Format - default) or "
                "'plaintext' (converted to plain text). Applies to both description and comments."
            ),
        )

        # Output options
        parser.add_argument(
            "--output-format",
            type=str,
            required=False,
            default="console",
            help=(
                "Output format: 'console', 'json', 'md', or comma-separated combination like 'json,md'. "
                "Default is 'console'."
            ),
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Output file path for JSON export. Auto-generated if not specified.",
        )
        parser.add_argument(
            "--summary-output",
            type=str,
            required=False,
            default="auto",
            choices=["auto", "json", "none"],
            help="Summary output mode: 'auto' (JSON if available), 'json' (force), 'none' (disable). Default: auto.",
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed information.",
        )

    @staticmethod
    def main(args: Namespace):
        # ALWAYS start with these two lines
        ensure_env_loaded()
        logger = LogManager.get_instance().get_logger("IssueSnapshotCommand")

        try:
            logger.info("Starting JIRA Issue Snapshot command")

            # Parse teams argument (handle comma-separated and repeated values)
            teams = None
            if args.teams:
                teams = []
                for team_arg in args.teams:
                    teams.extend([t.strip() for t in team_arg.split(",")])
                teams = [t for t in teams if t]  # Remove empty strings

            # Parse status arguments
            status_list = None
            if args.status:
                status_list = [s.strip() for s in args.status.split(",")]

            status_categories = None
            if args.status_categories:
                status_categories = [s.strip() for s in args.status_categories.split(",")]

            # Parse output formats
            output_formats = [fmt.strip().lower() for fmt in args.output_format.split(",")]

            # Delegate ALL business logic to service
            service = IssueSnapshotService()
            result = service.fetch_issue_snapshot(
                project_key=args.project_key,
                issue_type=args.issue_type,
                end_date=args.end_date,
                window_days=args.window_days,
                teams=teams,
                status=status_list,
                status_categories=status_categories,
                include_comments=args.comments,  # Now a boolean flag
                text_format=args.text_format,  # adf or plaintext, applies to both description and comments
                verbose=args.verbose,
                output_file=args.output_file,
                output_formats=output_formats,
            )

            # Emit summary using JiraSummaryManager
            summary_path = JiraSummaryManager().emit_summary_compatible(
                result,
                args.summary_output,
                result.get("output_file"),
                args,
            )

            if summary_path:
                logger.info(f"Summary saved to: {summary_path}")

            logger.info("Issue Snapshot command completed successfully")

        except Exception as e:
            logger.error(f"Issue Snapshot command failed: {e}", exc_info=True)
            exit(1)  # CLI commands MUST exit with error codes
