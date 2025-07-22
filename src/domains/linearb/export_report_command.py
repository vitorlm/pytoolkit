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
        return "Export and download performance reports from LinearB in CSV or JSON format"

    @staticmethod
    def get_help() -> str:
        return """
Export and automatically download performance reports from LinearB in CSV or JSON format.

This command exports comprehensive performance reports from LinearB API,
automatically downloads them, and saves them locally in the 'output' directory
for offline analysis.

Examples:
  # Export team performance report for last week in CSV format
  python src/main.py linearb export-report --team-ids 41576 --time-range last-week --format csv

  # Export contributor performance report for last month with custom output folder
  python src/main.py linearb export-report --filter-type contributor \\
    --time-range last-month --format json --output-folder /path/to/custom/folder

  # Export repository performance with weekly granularity and p75 aggregation
  python src/main.py linearb export-report --filter-type repository \\
    --time-range last-2-weeks --granularity 1w --aggregation p75

Available team IDs:
  - 19767: Core Services Tribe
  - 41576: Farm Operations Team

Available formats: csv, json
Available granularities: 1d, 1w, 1mo, custom
Available aggregations: p75, avg, p50, raw, default (default = no aggregation)
Available filter types: organization, contributor, team, repository, label, custom_metric

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
