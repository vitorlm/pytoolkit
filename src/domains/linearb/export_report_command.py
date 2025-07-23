"""
LinearB Export Performance Report Command.
"""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_linearb_env_loaded
from utils.logging.logging_manager import LogManager

from .linearb_service import LinearBService


class ExportReportCommand(BaseCommand):
    """Command to export performance reports from LinearB."""

    @staticmethod
    def get_name() -> str:
        return "export-report"

    @staticmethod
    def get_description() -> str:
        return (
            "Export comprehensive performance reports from LinearB including "
            "review time, deploy metrics, PR analytics"
        )

    @staticmethod
    def get_help() -> str:
        return """
Export and automatically download comprehensive performance reports from LinearB
in CSV or JSON format.

This command exports comprehensive performance reports from LinearB API,
automatically downloads them, and saves them locally in the 'output' directory
for offline analysis.

Included Metrics:
  • Review Time (branch.review_time) - Time spent in code review
  • Deploy Time (branch.time_to_prod) - Time from merge to production
  • Deploy Frequency (releases.count) - Number of releases/deployments
  • PR Size (pr.merged.size) - Average size of merged pull requests
  • PRs Merged Without Review (pr.merged.without.review.count) - Unreviewed merges
  • Review Depth (pr.review_depth) - Comments per PR ratio
  • PR Maturity (pr.maturity_ratio) - PR maturity scoring
  • Cycle Time, Time to PR, Time to Review, Time to Merge (core metrics)

Examples:
  # Basic export - team performance report for last week in CSV format
  python src/main.py linearb export-report --team-ids 41576 --time-range last-week --format csv

  # Advanced export with dashboard-compatible formatting and filtering
  python src/main.py linearb export-report --team-ids 41576 --time-range last-week \\
    --format csv --beautified --return-no-data --limit 100

  # Export contributor performance with specific contributors and ordering
  python src/main.py linearb export-report --filter-type contributor \\
    --contributor-ids 123,456,789 --time-range last-month --order-by name --order-dir desc

  # Export repository performance with service filtering and labels
  python src/main.py linearb export-report --filter-type repository \\
    --repository-ids 1001,1002 --service-ids 501,502 --labels backend,frontend \\
    --time-range last-2-weeks --granularity 1w

  # Export with custom output folder and pagination
  python src/main.py linearb export-report --time-range last-week \\
    --output-folder /custom/path --limit 50 --offset 100

Available team IDs:
  - 19767: Core Services Tribe
  - 41576: Farm Operations Team

Available formats: csv, json
Available granularities: 1d, 1w, 1mo, custom
Available aggregations: p75, avg, p50, raw, default (default = no aggregation)
Available filter types: organization, contributor, team, repository, label, custom_metric

New Dashboard-Compatible Parameters:
  --beautified: Format data for better readability (matches LinearB dashboard)
  --return-no-data: Include teams/contributors with no data
  --contributor-ids: Filter by specific contributors
  --repository-ids: Filter by specific repositories
  --service-ids: Filter by specific services
  --labels: Filter/group by labels (max 3)
  --limit: Limit response size
  --offset: Pagination offset
  --order-by: Sort by field name
  --order-dir: Sort direction (asc/desc)

Note: Reports are automatically downloaded to the specified output folder with
      timestamped filenames for easy identification.
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument("--team-ids", type=str, help="Comma-separated team IDs to filter by")

        parser.add_argument(
            "--time-range",
            type=str,
            required=True,
            help=(
                "Time range for the report (last-week, last-2-weeks, last-month, "
                "N-days, or YYYY-MM-DD,YYYY-MM-DD)"
            ),
        )

        parser.add_argument(
            "--granularity",
            type=str,
            choices=["1d", "1w", "1mo", "custom"],
            default="custom",
            help="Data granularity (default: custom)",
        )

        parser.add_argument(
            "--filter-type",
            type=str,
            choices=[
                "organization",
                "contributor",
                "team",
                "repository",
                "label",
                "custom_metric",
            ],
            default="team",
            help="Filter type for grouping data (default: team)",
        )

        parser.add_argument(
            "--aggregation",
            type=str,
            choices=["p75", "avg", "p50", "raw", "default"],
            default="default",
            help="Aggregation type for time-based metrics (default: default - no aggregation)",
        )

        parser.add_argument(
            "--format",
            type=str,
            choices=["csv", "json"],
            default="csv",
            help="Export format (default: csv)",
        )

        parser.add_argument(
            "--output-folder",
            type=str,
            default="output",
            help="Output folder to save the downloaded report (default: output)",
        )

        # New optional parameters matching dashboard capabilities
        parser.add_argument(
            "--beautified",
            action="store_true",
            default=True,
            help="Format data in a more readable format (default: True, matches dashboard)",
        )

        parser.add_argument(
            "--return-no-data",
            action="store_true",
            default=True,
            help="Return teams/contributors with no data (default: True)",
        )

        parser.add_argument(
            "--contributor-ids",
            type=str,
            help="Comma-separated contributor IDs to filter by",
        )

        parser.add_argument(
            "--repository-ids",
            type=str,
            help="Comma-separated repository IDs to filter by",
        )

        parser.add_argument(
            "--service-ids",
            type=str,
            help="Comma-separated service IDs to filter by",
        )

        parser.add_argument(
            "--labels",
            type=str,
            help="Comma-separated labels to filter/group by (max 3)",
        )

        parser.add_argument(
            "--limit",
            type=int,
            help="Maximum number of objects in the response",
        )

        parser.add_argument(
            "--offset",
            type=int,
            default=0,
            help="Pagination offset (default: 0)",
        )

        parser.add_argument(
            "--order-by",
            type=str,
            help="Field name to order results by",
        )

        parser.add_argument(
            "--order-dir",
            type=str,
            choices=["asc", "desc"],
            default="asc",
            help="Ordering direction (default: asc)",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_linearb_env_loaded()
        logger = LogManager.get_instance().get_logger("ExportReportCommand")

        try:
            service = LinearBService()

            logger.info("Exporting performance report from LinearB...")
            logger.info(f"Time range: {args.time_range}")
            logger.info(f"Format: {args.format}")
            logger.info(f"Filter type: {args.filter_type}")
            logger.info(f"Granularity: {args.granularity}")
            logger.info(f"Aggregation: {args.aggregation}")
            logger.info(f"Output folder: {args.output_folder}")

            if hasattr(args, "team_ids") and args.team_ids:
                logger.info(f"Team IDs: {args.team_ids}")
            if hasattr(args, "contributor_ids") and args.contributor_ids:
                logger.info(f"Contributor IDs: {args.contributor_ids}")
            if hasattr(args, "repository_ids") and args.repository_ids:
                logger.info(f"Repository IDs: {args.repository_ids}")
            if hasattr(args, "service_ids") and args.service_ids:
                logger.info(f"Service IDs: {args.service_ids}")
            if hasattr(args, "labels") and args.labels:
                logger.info(f"Labels: {args.labels}")
            if hasattr(args, "limit") and args.limit:
                logger.info(f"Limit: {args.limit}")
            if hasattr(args, "offset") and args.offset:
                logger.info(f"Offset: {args.offset}")
            if hasattr(args, "order_by") and args.order_by:
                logger.info(f"Order by: {args.order_by} ({args.order_dir})")

            logger.info(f"Beautified: {getattr(args, 'beautified', True)}")
            logger.info(f"Return no data: {getattr(args, 'return_no_data', True)}")

            # Export performance report
            export_result = service.export_performance_report(args)

            report_url = export_result.get("report_url")
            downloaded_file = export_result.get("downloaded_file")
            download_error = export_result.get("download_error")
            detail = export_result.get("detail", "Export completed")

            if report_url:
                logger.info("Export successful!")
                logger.info(f"Download URL: {report_url}")
                logger.info(f"Details: {detail}")
                logger.info("")

                if downloaded_file:
                    logger.info("✅ Report downloaded and saved successfully!")
                    logger.info(f"📁 Local file: {downloaded_file}")
                    logger.info("You can now analyze the data locally.")
                elif download_error:
                    logger.warning("⚠️  Export succeeded but download failed:")
                    logger.warning(f"Error: {download_error}")
                    logger.info("You can still download the report manually using the URL above.")
                else:
                    logger.info("You can download the report using the provided URL.")
                    logger.info("The report will be available for a limited time.")
            else:
                logger.info(f"Export initiated: {detail}")
                logger.info(
                    "The report may still be processing. Check back later or "
                    "monitor your LinearB notifications."
                )

            logger.info("Export report command completed successfully")

        except Exception as e:
            logger.error(f"Export report command failed: {e}")
            exit(1)
