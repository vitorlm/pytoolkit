"""
LinearB Service for performance metrics and team analytics operations.
"""

import os
from argparse import Namespace
from typing import Any, Dict, List, Optional

from utils.data.json_manager import JSONManager
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager

from .linearb_api_client import (
    LinearBAggregation,
    LinearBApiClient,
    LinearBGroupBy,
    LinearBMetrics,
    LinearBRollup,
    LinearBTimeRangeHelper,
)


class LinearBService:
    """Service for LinearB performance metrics and analytics operations."""

    def __init__(self):
        """Initialize the LinearB service."""
        self.logger = LogManager.get_instance().get_logger("LinearBService")
        self.api_client = LinearBApiClient()
        self.time_helper = LinearBTimeRangeHelper()

    def test_connection(self) -> Dict[str, Any]:
        """
        Test the connection to LinearB API.

        Returns:
            Dictionary with connection test results.
        """
        return self.api_client.test_connection()

    def get_performance_metrics(self, args: Namespace) -> Dict[str, Any]:
        """
        Get performance metrics from LinearB based on the provided arguments.

        Args:
            args: Command arguments containing the following:
                - time_range: Time period to analyze (last-week, last-2-weeks, etc.)
                - team_ids: List of team IDs (optional)
                - aggregation: Aggregation type (p75, p50, avg)
                - granularity: Data granularity (1d, 1w, 1mo, custom)
                - filter_type: Filter type (organization, contributor, team, repository,
                  label, custom_metric)
                - save_results: Boolean to save results to file

        Returns:
            Performance metrics data
        """
        try:
            # Parse time ranges
            time_ranges = self.time_helper.parse_time_period(args.time_range)

            # Map granularity to rollup (now using API-correct values)
            granularity_map = {
                "custom": LinearBRollup.CUSTOM,
                "1d": LinearBRollup.DAILY,
                "1w": LinearBRollup.WEEKLY,
                "1mo": LinearBRollup.MONTHLY,
            }
            roll_up = granularity_map.get(args.granularity, LinearBRollup.CUSTOM)

            # Map filter type to group_by (now using API-correct values)
            filter_type_map = {
                "organization": LinearBGroupBy.ORGANIZATION,
                "contributor": LinearBGroupBy.CONTRIBUTOR,
                "team": LinearBGroupBy.TEAM,
                "repository": LinearBGroupBy.REPOSITORY,
                "label": LinearBGroupBy.LABEL,
                "custom_metric": LinearBGroupBy.CUSTOM_METRIC,
            }
            group_by = filter_type_map.get(args.filter_type, LinearBGroupBy.TEAM)

            # Get aggregation with default
            aggregation = getattr(args, "aggregation", LinearBAggregation.DEFAULT)

            # Default metrics if not specified
            default_metrics = [
                {"name": LinearBMetrics.CYCLE_TIME, "agg": aggregation},
                {"name": LinearBMetrics.TIME_TO_PR, "agg": aggregation},
                {"name": LinearBMetrics.TIME_TO_REVIEW, "agg": aggregation},
                {"name": LinearBMetrics.TIME_TO_MERGE, "agg": aggregation},
                {"name": LinearBMetrics.PR_MERGED, "agg": LinearBAggregation.DEFAULT},
                {"name": LinearBMetrics.PR_NEW, "agg": LinearBAggregation.DEFAULT},
                {
                    "name": LinearBMetrics.COMMIT_TOTAL_COUNT,
                    "agg": LinearBAggregation.DEFAULT,
                },
                {
                    "name": LinearBMetrics.RELEASES_COUNT,
                    "agg": LinearBAggregation.DEFAULT,
                },
            ]

            requested_metrics = getattr(args, "metrics", default_metrics)
            if isinstance(requested_metrics, list) and all(isinstance(m, str) for m in requested_metrics):
                # Convert string list to metric objects
                requested_metrics = [{"name": metric, "agg": aggregation} for metric in requested_metrics]

            # Parse team IDs if provided
            team_ids = None
            if hasattr(args, "team_ids") and args.team_ids:
                if isinstance(args.team_ids, str):
                    team_ids = [int(tid) for tid in args.team_ids.split(",")]
                elif isinstance(args.team_ids, list):
                    team_ids = [int(tid) for tid in args.team_ids]
                else:
                    team_ids = [int(args.team_ids)]

            self.logger.info(f"Fetching metrics for time range: {args.time_range}")
            self.logger.info(f"Group by: {group_by}, Roll up: {roll_up}")
            if team_ids:
                self.logger.info(f"Team IDs filter: {team_ids}")

            # Get metrics from LinearB
            metrics_data = self.api_client.get_metrics(
                requested_metrics=requested_metrics,
                time_ranges=time_ranges,
                group_by=group_by,
                team_ids=team_ids,
                roll_up=roll_up,
            )

            self.logger.info(f"Retrieved {len(metrics_data)} metric time periods")

            return {
                "metrics": metrics_data,
                "parameters": {
                    "time_range": args.time_range,
                    "granularity": getattr(args, "granularity", "auto"),
                    "filter_type": getattr(args, "filter_type", "team"),
                    "team_ids": team_ids,
                    "group_by": group_by,
                    "roll_up": roll_up,
                    "aggregation": aggregation,
                },
                "time_ranges": time_ranges,
                "requested_metrics": requested_metrics,
            }

        except Exception as e:
            self.logger.error(f"Failed to get performance metrics: {e}", exc_info=True)
            raise

    def get_teams_info(self, search_term: Optional[str] = None) -> Dict[str, Any]:
        """
        Get teams information from LinearB.

        Args:
            search_term: Optional search term to filter teams

        Returns:
            Teams information
        """
        try:
            self.logger.info("Fetching teams information from LinearB")

            teams_data = self.api_client.get_teams(search_term=search_term)

            self.logger.info(f"Retrieved {teams_data.get('total', 0)} teams")

            return teams_data

        except Exception as e:
            self.logger.error(f"Failed to get teams information: {e}", exc_info=True)
            raise

    def export_performance_report(self, args: Namespace) -> Dict[str, Any]:
        """
        Export performance metrics report from LinearB.

        Args:
            args: Command arguments

        Returns:
            Export result with download URL
        """
        try:
            # Parse time ranges
            time_ranges = self.time_helper.parse_time_period(args.time_range)

            # Map granularity to rollup (now using API-correct values)
            granularity_map = {
                "custom": LinearBRollup.CUSTOM,
                "1d": LinearBRollup.DAILY,
                "1w": LinearBRollup.WEEKLY,
                "1mo": LinearBRollup.MONTHLY,
            }
            roll_up = granularity_map.get(args.granularity, LinearBRollup.CUSTOM)

            # Map filter type to group_by (now using API-correct values)
            filter_type_map = {
                "organization": LinearBGroupBy.ORGANIZATION,
                "contributor": LinearBGroupBy.CONTRIBUTOR,
                "team": LinearBGroupBy.TEAM,
                "repository": LinearBGroupBy.REPOSITORY,
                "label": LinearBGroupBy.LABEL,
                "custom_metric": LinearBGroupBy.CUSTOM_METRIC,
            }
            group_by = filter_type_map.get(args.filter_type, LinearBGroupBy.TEAM)

            # Get aggregation with default
            aggregation = getattr(args, "aggregation", LinearBAggregation.DEFAULT)

            # Default metrics for export - including requested metrics
            export_metrics = [
                # Core cycle time metrics
                {"name": LinearBMetrics.CYCLE_TIME, "agg": aggregation},
                {"name": LinearBMetrics.TIME_TO_PR, "agg": aggregation},
                {"name": LinearBMetrics.TIME_TO_REVIEW, "agg": aggregation},
                {"name": LinearBMetrics.TIME_TO_MERGE, "agg": aggregation},
                # Requested metrics from user
                {"name": LinearBMetrics.REVIEW_TIME, "agg": aggregation},  # Review Time
                {"name": LinearBMetrics.TIME_TO_PROD, "agg": aggregation},  # Deploy Time
                {"name": LinearBMetrics.RELEASES_COUNT},  # Deploy frequency
                {"name": LinearBMetrics.PR_MERGED_SIZE, "agg": aggregation},  # PR Size
                {"name": LinearBMetrics.PR_MERGED_WITHOUT_REVIEW},  # PRs merged w/o review
                {"name": LinearBMetrics.PR_REVIEW_DEPTH},  # Review Depth
                {"name": LinearBMetrics.PR_MATURITY},  # PR Maturity
                # Additional useful metrics
                {"name": LinearBMetrics.PR_MERGED, "agg": LinearBAggregation.DEFAULT},
                {"name": LinearBMetrics.PR_NEW, "agg": LinearBAggregation.DEFAULT},
                {
                    "name": LinearBMetrics.COMMIT_TOTAL_COUNT,
                    "agg": LinearBAggregation.DEFAULT,
                },
            ]

            # Parse team IDs if provided
            team_ids = None
            if hasattr(args, "team_ids") and args.team_ids:
                if isinstance(args.team_ids, str):
                    team_ids = [int(tid) for tid in args.team_ids.split(",")]
                elif isinstance(args.team_ids, list):
                    team_ids = [int(tid) for tid in args.team_ids]
                else:
                    team_ids = [int(args.team_ids)]

            # Parse contributor IDs if provided
            contributor_ids = None
            if hasattr(args, "contributor_ids") and args.contributor_ids:
                contributor_ids = [int(cid) for cid in args.contributor_ids.split(",")]

            # Parse repository IDs if provided
            repository_ids = None
            if hasattr(args, "repository_ids") and args.repository_ids:
                repository_ids = [int(rid) for rid in args.repository_ids.split(",")]

            # Parse service IDs if provided
            service_ids = None
            if hasattr(args, "service_ids") and args.service_ids:
                service_ids = [int(sid) for sid in args.service_ids.split(",")]

            # Parse labels if provided
            labels = None
            if hasattr(args, "labels") and args.labels:
                labels = [label.strip() for label in args.labels.split(",")][:3]  # Max 3 labels

            file_format = getattr(args, "format", "json")

            self.logger.info(f"Exporting performance report for time range: {args.time_range}")
            self.logger.info(f"Format: {file_format}, Group by: {group_by}")

            # Use dashboard-compatible parameters for better data quality
            export_result = self.api_client.export_metrics(
                requested_metrics=export_metrics,
                time_ranges=time_ranges,
                group_by=group_by,
                team_ids=team_ids,
                file_format=file_format,
                roll_up=roll_up,
                beautified=getattr(args, "beautified", True),
                return_no_data=getattr(args, "return_no_data", True),
                contributor_ids=contributor_ids,
                repository_ids=repository_ids,
                service_ids=service_ids,
                labels=labels,
                limit=getattr(args, "limit", None),
                offset=getattr(args, "offset", 0),
                order_by=getattr(args, "order_by", None),
                order_dir=getattr(args, "order_dir", "asc"),
            )

            self.logger.info(f"Export completed. Report URL: {export_result.get('report_url', 'N/A')}")

            # If we have a report URL, download the file automatically
            report_url = export_result.get("report_url")
            if report_url:
                try:
                    output_folder = getattr(args, "output_folder", "output")
                    downloaded_file = self.download_and_save_report(
                        report_url=report_url,
                        file_format=file_format,
                        team_ids=team_ids,
                        output_folder=output_folder,
                    )
                    export_result["downloaded_file"] = downloaded_file
                    self.logger.info(f"Report downloaded and saved to: {downloaded_file}")
                except Exception as download_error:
                    self.logger.error(f"Failed to download report: {download_error}")
                    # Don't fail the entire operation if download fails
                    export_result["download_error"] = str(download_error)

            return export_result

        except Exception as e:
            self.logger.error(f"Failed to export performance report: {e}", exc_info=True)
            raise

    def download_and_save_report(
        self,
        report_url: str,
        file_format: str = "csv",
        team_ids: Optional[List[int]] = None,
        output_folder: str = "output",
    ) -> str:
        """
        Downloads a report from the provided URL and saves it locally.

        Args:
            report_url: The URL to download the report from
            file_format: The format of the report (csv, json)
            team_ids: Optional team IDs for filename generation
            output_folder: The output folder to save the file (default: "output")

        Returns:
            Path to the downloaded file

        Raises:
            Exception: If download or save fails
        """
        try:
            # Generate a filename
            team_suffix = f"_teams_{'_'.join(map(str, team_ids))}" if team_ids else ""
            extension = f".{file_format}"

            filename = FileManager.generate_file_name(
                module="linearb_report",
                suffix=f"export{team_suffix}",
                extension=extension,
                include_timestamp=True,
            )

            # Ensure output directory exists
            FileManager.create_folder(output_folder, exist_ok=True)

            destination_path = os.path.join(output_folder, filename)

            self.logger.info(f"Downloading report from: {report_url}")
            self.logger.info(f"Saving to: {destination_path}")

            # Download the file using FileManager's new download method
            downloaded_path = FileManager.download_file(
                url=report_url,
                destination_path=destination_path,
                timeout=60,  # Longer timeout for report downloads
            )

            self.logger.info(f"Report successfully downloaded and saved to: {downloaded_path}")

            # Get file size for confirmation
            if FileManager.file_exists(downloaded_path):
                metadata = FileManager.retrieve_metadata(downloaded_path)
                self.logger.info(f"File size: {metadata['size']}")

            return downloaded_path

        except Exception as e:
            self.logger.error(f"Failed to download and save report: {e}", exc_info=True)
            raise

    def save_metrics_to_file(self, metrics_data: Dict[str, Any], output_file: Optional[str] = None) -> str:
        """
        Save metrics data to a JSON file.

        Args:
            metrics_data: Metrics data to save
            output_file: Optional output file path

        Returns:
            Path to the saved file
        """
        try:
            if not output_file:
                # Generate filename based on parameters
                params = metrics_data.get("parameters", {})
                time_range = params.get("time_range", "unknown").replace(",", "_to_")
                filter_type = params.get("filter_type", "unknown")

                output_file = FileManager.generate_file_name(
                    module="linearb",
                    suffix=f"performance_{filter_type}_{time_range}",
                    extension=".json",
                )

            # Ensure output directory exists
            FileManager.create_folder("output")
            full_path = f"output/{output_file}"

            # Save the data
            JSONManager.write_json(metrics_data, full_path)

            self.logger.info(f"Metrics data saved to {full_path}")
            return full_path

        except Exception as e:
            self.logger.error(f"Failed to save metrics data: {e}", exc_info=True)
            raise

    def get_available_metrics(self) -> Dict[str, List[str]]:
        """
        Get a list of available metrics organized by category.

        Returns:
            Dictionary of available metrics by category
        """
        return {
            "time_metrics": [
                LinearBMetrics.TIME_TO_PR,
                LinearBMetrics.TIME_TO_APPROVE,
                LinearBMetrics.TIME_TO_MERGE,
                LinearBMetrics.TIME_TO_REVIEW,
                LinearBMetrics.REVIEW_TIME,
                LinearBMetrics.TIME_TO_PROD,
                LinearBMetrics.CYCLE_TIME,
            ],
            "pr_metrics": [
                LinearBMetrics.PR_MERGED,
                LinearBMetrics.PR_MERGED_SIZE,
                LinearBMetrics.PR_NEW,
                LinearBMetrics.PR_REVIEW_DEPTH,
                LinearBMetrics.PR_REVIEWS,
                LinearBMetrics.PR_MERGED_WITHOUT_REVIEW,
                LinearBMetrics.PR_REVIEWED,
            ],
            "commit_metrics": [
                LinearBMetrics.COMMIT_TOTAL_COUNT,
                LinearBMetrics.COMMIT_NEW_WORK,
                LinearBMetrics.COMMIT_REWORK,
                LinearBMetrics.COMMIT_REFACTOR,
                LinearBMetrics.COMMIT_TOTAL_CHANGES,
                LinearBMetrics.COMMIT_ACTIVITY_DAYS,
                LinearBMetrics.COMMIT_INVOLVED_REPOS,
            ],
            "other_metrics": [
                LinearBMetrics.RELEASES_COUNT,
                LinearBMetrics.BRANCH_STATE_DONE,
                LinearBMetrics.BRANCH_STATE_ACTIVE,
                LinearBMetrics.CONTRIBUTOR_CODING_DAYS,
            ],
        }

    def get_team_performance_summary(self, metrics_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate a performance summary from metrics data.

        Args:
            metrics_data: Raw metrics data from LinearB

        Returns:
            Performance summary
        """
        try:
            summary = {
                "total_time_periods": len(metrics_data.get("metrics", [])),
                "parameters": metrics_data.get("parameters", {}),
                "teams_summary": [],
                "metrics_summary": {},
            }

            # Process each time period
            for time_period in metrics_data.get("metrics", []):
                period_info = {
                    "period": (f"{time_period.get('after', 'Unknown')} to {time_period.get('before', 'Unknown')}"),
                    "teams_count": len(time_period.get("metrics", [])),
                    "metrics_available": [],
                }

                # Collect available metrics
                for metric_data in time_period.get("metrics", []):
                    for key in metric_data.keys():
                        metrics_available = period_info["metrics_available"]
                        if isinstance(metrics_available, list):
                            if key not in ["id", "name", "type"] and key not in metrics_available:
                                metrics_available.append(key)

                summary["teams_summary"].append(period_info)

            self.logger.info("Generated performance summary")
            return summary

        except Exception as e:
            self.logger.error(f"Failed to generate performance summary: {e}", exc_info=True)
            raise

    def get_engineering_metrics(self, args: Namespace) -> None:
        """
        Retrieve software engineering metrics from LinearB API.

        Args:
            args: Command arguments containing filters and options
        """
        self.logger.info("Starting engineering metrics retrieval")

        # Map the 9 specific engineering metrics requested
        # Get aggregation type from arguments (default, p75, p50, avg, raw)
        aggregation = getattr(args, "aggregation", "default")

        metrics = [
            {"name": LinearBMetrics.PR_MERGED_WITHOUT_REVIEW, "agg": "default"},
            {"name": LinearBMetrics.PR_REVIEW_DEPTH, "agg": aggregation},
            {"name": LinearBMetrics.PR_MATURITY, "agg": aggregation},
            {"name": LinearBMetrics.DEPLOY_TIME, "agg": aggregation},
            {"name": LinearBMetrics.PR_SIZE, "agg": aggregation},
            {"name": LinearBMetrics.DEPLOY_FREQUENCY, "agg": "default"},
            {"name": LinearBMetrics.CYCLE_TIME, "agg": aggregation},
            {"name": LinearBMetrics.PICKUP_TIME, "agg": aggregation},
            {"name": LinearBMetrics.REVIEW_TIME, "agg": aggregation},
        ]

        # Parse time ranges using the time helper
        time_ranges = self.time_helper.parse_time_period(args.time_range)

        # Parse team IDs if provided
        team_ids = None
        if hasattr(args, "team_ids") and args.team_ids:
            if isinstance(args.team_ids, str):
                team_ids = [int(tid) for tid in args.team_ids.split(",")]
            elif isinstance(args.team_ids, list):
                team_ids = [int(tid) for tid in args.team_ids]
            else:
                team_ids = [int(args.team_ids)]

        # Set group_by based on filter_type
        group_by = "team"  # default
        if hasattr(args, "filter_type") and args.filter_type:
            filter_type_map = {
                "team": "team",
                "contributor": "contributor",
                "repository": "repository",
                "organization": "organization",
            }
            group_by = filter_type_map.get(args.filter_type, "team")

        # Set roll_up based on granularity
        roll_up = "custom"  # default
        if hasattr(args, "granularity") and args.granularity:
            granularity_map = {"1d": "1d", "1w": "1w", "1mo": "1mo", "custom": "custom"}
            roll_up = granularity_map.get(args.granularity, "custom")

        try:
            self.logger.info("Fetching engineering metrics from LinearB API")

            # Fetch all metrics in one API call
            metrics_data = self.api_client.get_metrics(
                requested_metrics=metrics,
                time_ranges=time_ranges,
                group_by=group_by,
                team_ids=team_ids,
                roll_up=roll_up,
            )

            if not metrics_data:
                self.logger.warning("No metrics data retrieved")
                return

            self.logger.info("Successfully retrieved engineering metrics")

            # Save data in requested format
            output_format = getattr(args, "format", "json").lower()
            output_folder = getattr(args, "output_folder", "output")

            # Create output folder if it doesn't exist
            FileManager.create_folder(output_folder)

            # Generate filename with timestamp
            if output_format == "csv":
                filename_base = FileManager.generate_file_name(
                    module="linearb_engineering_metrics", suffix="data", extension=".csv"
                )
                csv_file = f"{output_folder}/{filename_base}"
                self._save_metrics_as_csv(metrics_data, csv_file)
            else:
                # Save as JSON (default)
                filename_base = FileManager.generate_file_name(
                    module="linearb_engineering_metrics", suffix="data", extension=".json"
                )
                json_file = f"{output_folder}/{filename_base}"
                JSONManager.write_json(metrics_data, json_file)
                self.logger.info(f"Engineering metrics data saved to {json_file}")

            self.logger.info("Engineering metrics retrieval completed")

        except Exception as e:
            self.logger.error(f"Error fetching engineering metrics: {e}")
            raise

    def _save_metrics_as_csv(self, metrics_data: Any, file_path: str) -> None:
        """Save metrics data to CSV format."""
        import pandas as pd

        # Flatten the LinearB metrics data for CSV format
        rows = []

        # Handle different response formats from LinearB API
        if isinstance(metrics_data, list):
            # metrics_data is a list of time periods from LinearB API
            for time_period in metrics_data:
                period_start = time_period.get("after", "")
                period_end = time_period.get("before", "")
                # Compare start and end dates, skip if they're the same
                if period_start == period_end:
                    self.logger.info(f"Skipping time period with identical start/end dates: {period_start}")
                    continue
                # Each time period contains metrics for teams/contributors
                for team_metrics in time_period.get("metrics", []):
                    team_id = team_metrics.get("id", "")
                    team_name = team_metrics.get("name", "")
                    team_type = team_metrics.get("type", "")

                    # Extract all metric values for this team/period
                    base_row = {
                        "period_start": period_start,
                        "period_end": period_end,
                        "team_id": team_id,
                        "team_name": team_name,
                        "team_type": team_type,
                    }

                    # Add all the metric values as columns
                    for key, value in team_metrics.items():
                        if key not in ["id", "name", "type"]:
                            base_row[key] = value

                    rows.append(base_row)

        elif isinstance(metrics_data, dict):
            # Handle dict format (fallback for other API responses)
            for metric_name, metric_data in metrics_data.items():
                if isinstance(metric_data, dict) and "data" in metric_data:
                    data_points = metric_data.get("data", [])
                    for point in data_points:
                        row = {
                            "metric_name": metric_name,
                            "timestamp": point.get("timestamp", ""),
                            "value": point.get("value", ""),
                            "team_id": point.get("team_id", ""),
                        }
                        # Add any additional fields
                        for key, value in point.items():
                            if key not in ["timestamp", "value", "team_id"]:
                                row[key] = value
                        rows.append(row)

        if rows:
            df = pd.DataFrame(rows)
            df.to_csv(file_path, index=False)
            self.logger.info(f"Engineering metrics data saved to {file_path}")
        else:
            self.logger.warning("No data to save to CSV format")
