"""GitHub Pull Request Workload Analysis Service

Business logic for analyzing PR data from files to evaluate CODEOWNERS workload
and assess if they are under pressure due to increased PRs from external authors.
"""

import csv
import os
from argparse import Namespace
from datetime import datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # Set non-interactive backend before importing pyplot
import matplotlib.pyplot as plt
import pandas as pd

from utils.data.json_manager import JSONManager
from utils.error.error_manager import handle_generic_exception
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager


class PrWorkloadAnalysisService:
    """Service for analyzing PR workload data and evaluating CODEOWNERS pressure."""

    def __init__(self):
        self.logger = LogManager.get_instance().get_logger("PrWorkloadAnalysisService")

    def _calculate_business_days(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
        """Calculate the number of business days between two dates (excluding weekends)."""
        if pd.isna(start_date) or pd.isna(end_date):
            return 0

        # Use pandas bdate_range to get business days
        business_days = len(pd.bdate_range(start=start_date.date(), end=end_date.date()))
        return max(business_days - 1, 0)  # Subtract 1 to get the count of days in between

    def _get_date_range_business_days(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> int:
        """Get the number of business days in a date range."""
        if pd.isna(start_date) or pd.isna(end_date):
            return 0

        return len(pd.bdate_range(start=start_date.date(), end=end_date.date()))

    def _calculate_monthly_work_day_rate(self, month_period: pd.Period, pr_count: int) -> float:
        """Calculate PRs per work day for a specific month."""
        # Convert Period to start and end dates
        start_date = month_period.to_timestamp()
        end_date = month_period.to_timestamp(how="end")

        # Get business days in the month
        business_days_in_month = self._get_date_range_business_days(start_date, end_date)

        # Calculate rate (avoid division by zero)
        return pr_count / max(business_days_in_month, 1)

    def _calculate_analysis_period_months(self, df: pd.DataFrame) -> float:
        """Calculate the analysis period in months."""
        if "created_at" not in df.columns or len(df) == 0:
            return 0

        min_date = df["created_at"].min()
        max_date = df["created_at"].max()

        # Calculate difference in months (approximate)
        days_diff = (max_date - min_date).days
        months_diff = days_diff / 30.44  # Average days per month

        return max(months_diff, 0.1)  # At least 0.1 month to avoid division issues

    def _filter_by_date_range(self, df: pd.DataFrame, args) -> pd.DataFrame:
        """Filter dataframe by date range if start_date and/or end_date are specified."""
        if not hasattr(args, "start_date") or not hasattr(args, "end_date"):
            return df

        start_date = getattr(args, "start_date", None)
        end_date = getattr(args, "end_date", None)

        # If no date filtering is requested, return original data
        if not start_date and not end_date:
            return df

        original_count = len(df)
        filtered_df = df.copy()

        # Check if created_at column has timezone information
        has_timezone = hasattr(filtered_df["created_at"].dtype, "tz") and filtered_df["created_at"].dtype.tz is not None

        # Apply start date filter
        if start_date:
            start_dt = pd.to_datetime(start_date)
            # Make timezone-aware if the data has timezone info
            if has_timezone:
                start_dt = start_dt.tz_localize("UTC") if start_dt.tz is None else start_dt.tz_convert("UTC")
            filtered_df = filtered_df[filtered_df["created_at"] >= start_dt]
            self.logger.info(f"Filtered by start date {start_date}: {len(filtered_df)} PRs remaining")

        # Apply end date filter (if not provided, use current date)
        if start_date and not end_date:
            end_dt = pd.Timestamp.now()
            # Make timezone-aware if the data has timezone info
            if has_timezone:
                end_dt = end_dt.tz_localize("UTC") if end_dt.tz is None else end_dt.tz_convert("UTC")
            filtered_df = filtered_df[filtered_df["created_at"] <= end_dt]
            self.logger.info(f"Using current date as end date: {end_dt.date()}")
        elif end_date:
            end_dt = pd.to_datetime(end_date)
            # Make timezone-aware if the data has timezone info
            if has_timezone:
                end_dt = end_dt.tz_localize("UTC") if end_dt.tz is None else end_dt.tz_convert("UTC")
            filtered_df = filtered_df[filtered_df["created_at"] <= end_dt]
            self.logger.info(f"Filtered by end date {end_date}: {len(filtered_df)} PRs remaining")

        if len(filtered_df) != original_count:
            self.logger.info(f"Date filtering applied: {original_count} -> {len(filtered_df)} PRs")

        if len(filtered_df) == 0:
            self.logger.warning("Date filtering resulted in no PRs remaining")

        return filtered_df

    def _convert_to_json_serializable(self, data: Any) -> Any:
        """Convert pandas/numpy data types to JSON-serializable Python native types."""
        if isinstance(data, dict):
            return {key: self._convert_to_json_serializable(value) for key, value in data.items()}
        elif isinstance(data, list):
            return [self._convert_to_json_serializable(item) for item in data]
        elif isinstance(data, (pd.Series, pd.DataFrame)):
            # Convert pandas objects to dict/list first, then recursively convert
            if isinstance(data, pd.DataFrame):
                return self._convert_to_json_serializable(data.to_dict("records"))
            else:
                return self._convert_to_json_serializable(data.tolist())
        elif hasattr(data, "dtype"):  # pandas/numpy scalars
            # Convert pandas/numpy types to native Python types
            if pd.isna(data):
                return None
            elif hasattr(data, "item"):  # numpy scalars
                return data.item()
            else:
                return data
        elif type(data).__module__ == "numpy":
            # Handle numpy types by converting to native Python types
            if hasattr(data, "item"):
                return data.item()
            else:
                return data.tolist() if hasattr(data, "tolist") else data
        elif pd.isna(data):
            return None
        else:
            return data

    def analyze_pr_workload(self, args: Namespace) -> dict[str, Any]:
        """Main method to analyze PR workload from file data.

        Args:
            args: Command line arguments from ArgumentParser

        Returns:
            Dictionary with analysis results and metadata
        """
        try:
            self.logger.info("Starting PR workload analysis")

            # Step 1: Load and validate data
            pr_data = self._load_pr_data(args.file, args.date_format)
            self._validate_data(pr_data, args.min_records)

            # Step 2: Prepare data for analysis
            df = self._prepare_dataframe(pr_data)

            # Step 2.5: Filter data by date range if specified
            df = self._filter_by_date_range(df, args)

            # Step 3: Calculate monthly trends
            monthly_trends = self._calculate_monthly_trends(df)

            # Step 4: Calculate correlations
            correlations = self._calculate_correlations(df)

            # Step 5: Analyze CODEOWNERS pressure metrics
            pressure_metrics = self._analyze_codeowners_pressure(df, args)

            # Step 5.5: NEW - Analyze specific team members workload
            team_analysis = self._analyze_team_members_workload(df)

            # Step 6: Generate insights and recommendations
            insights = self._generate_insights(df, monthly_trends, correlations, pressure_metrics, team_analysis)

            # Add pressure formula to correlations for reporting
            correlations["pressure_formula"] = pressure_metrics.get("pressure_formula", {})

            # Step 7: Create output directory
            output_dir = self._ensure_output_directory(args.output)

            # Step 8: Save results
            output_files = self._save_results(
                df,
                monthly_trends,
                correlations,
                pressure_metrics,
                team_analysis,
                insights,
                output_dir,
                args.detailed_analysis,
            )

            # Step 9: Generate charts if requested
            if args.generate_charts:
                chart_files = self._generate_charts(df, monthly_trends, correlations, output_dir)
                output_files.extend(chart_files)

            # Step 10: Generate markdown report
            report_file = self._generate_markdown_report(
                df,
                monthly_trends,
                correlations,
                pressure_metrics,
                team_analysis,
                insights,
                output_dir,
                args.generate_charts,
            )
            output_files.append(report_file)

            # Step 11: Generate HTML dashboard
            dashboard_file = self._generate_html_dashboard(
                df,
                monthly_trends,
                correlations,
                pressure_metrics,
                team_analysis,
                insights,
                output_dir,
            )
            output_files.append(dashboard_file)

            # Step 12: Compile results summary
            results = {
                "total_prs": len(df),
                "external_prs": len(df[~df["is_team_member"]]),
                "team_member_prs": len(df[df["is_team_member"]]),
                "analysis_period": self._get_analysis_period(df),
                "monthly_trends": monthly_trends,
                "correlations": correlations,
                "pressure_metrics": pressure_metrics,
                "team_analysis": team_analysis,
                "key_insights": insights["key_points"],
                "recommendations": insights["recommendations"],
                "output_files": output_files,
            }

            self.logger.info("PR workload analysis completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
            handle_generic_exception(e, "PR workload analysis", {"file": args.file})
            raise

    def _load_pr_data(self, file_path: str, date_format: str) -> list[dict[str, Any]]:
        """Load PR data from JSON or CSV file."""
        self.logger.info(f"Loading PR data from: {file_path}")

        # Validate file exists
        FileManager.validate_file(file_path, allowed_extensions=[".json", ".csv"])

        file_ext = Path(file_path).suffix.lower()

        if file_ext == ".json":
            return self._load_json_data(file_path)
        elif file_ext == ".csv":
            return self._load_csv_data(file_path, date_format)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

    def _load_json_data(self, file_path: str) -> list[dict[str, Any]]:
        """Load data from JSON file."""
        try:
            data = JSONManager.read_json(file_path)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and "prs" in data:
                return data["prs"]
            elif isinstance(data, dict) and "pull_requests" in data:
                return data["pull_requests"]
            else:
                raise ValueError("JSON file must contain a list of PR records or have a 'prs' or 'pull_requests' key")
        except Exception as e:
            raise ValueError(f"Failed to load JSON data: {e}")

    def _load_csv_data(self, file_path: str, date_format: str) -> list[dict[str, Any]]:
        """Load data from CSV file."""
        try:
            data = []
            with open(file_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Convert string booleans
                    if "is_team_member" in row:
                        row["is_team_member"] = row["is_team_member"].lower() in (
                            "true",
                            "1",
                            "yes",
                        )

                    # Convert numeric fields
                    numeric_fields = [
                        "lead_time_days",
                        "lead_time_hours",
                        "lead_time_seconds",
                        "additions",
                        "deletions",
                        "changed_files",
                        "commits",
                        "reviews_count",
                        "review_comments",
                        "issue_comments",
                        "approvals_count",
                        "requested_reviewers_count",
                        "time_to_first_review_seconds",
                        "first_response_latency_seconds",
                        "review_rounds",
                        "synchronize_after_first_review",
                        "re_review_pushes",
                    ]
                    for field in numeric_fields:
                        if row.get(field):
                            try:
                                row[field] = float(row[field]) if "." in str(row[field]) else int(row[field])
                            except ValueError:
                                row[field] = 0

                    data.append(row)
            return data
        except Exception as e:
            raise ValueError(f"Failed to load CSV data: {e}")

    def _validate_data(self, data: list[dict[str, Any]], min_records: int):
        """Validate the loaded data has required fields and minimum records."""
        if len(data) < min_records:
            raise ValueError(f"Insufficient data: {len(data)} records, minimum required: {min_records}")

        required_fields = ["author", "created_at", "is_team_member", "lead_time_days"]

        # Check first few records for required fields
        sample_size = min(5, len(data))
        for i, record in enumerate(data[:sample_size]):
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                raise ValueError(f"Record {i + 1} missing required fields: {missing_fields}")

        self.logger.info(f"Data validation passed: {len(data)} records with required fields")

    def _prepare_dataframe(self, data: list[dict[str, Any]]) -> pd.DataFrame:
        """Convert data to pandas DataFrame and prepare for analysis."""
        df = pd.DataFrame(data)

        # Convert datetime fields
        datetime_fields = ["created_at", "merged_at", "closed_at"]
        for field in datetime_fields:
            if field in df.columns:
                df[field] = pd.to_datetime(df[field], errors="coerce")

        # Create derived fields
        df["pr_size"] = df.get("additions", 0) + df.get("deletions", 0)
        df["month_year"] = df["created_at"].dt.to_period("M")

        # Fill missing numeric values with 0
        numeric_fields = [
            "lead_time_days",
            "lead_time_hours",
            "lead_time_seconds",
            "additions",
            "deletions",
            "changed_files",
            "commits",
            "reviews_count",
            "review_comments",
            "issue_comments",
            "approvals_count",
            "requested_reviewers_count",
            "time_to_first_review_seconds",
            "first_response_latency_seconds",
            "review_rounds",
            "synchronize_after_first_review",
            "re_review_pushes",
        ]
        for field in numeric_fields:
            if field in df.columns:
                df[field] = df[field].fillna(0)

        # Sort by creation date
        df = df.sort_values("created_at")

        self.logger.info(f"Prepared DataFrame with {len(df)} records and {len(df.columns)} columns")
        return df

    def _calculate_monthly_trends(self, df: pd.DataFrame) -> dict[str, Any]:
        """Calculate monthly trends for PR counts and lead times."""
        self.logger.info("Calculating monthly trends")

        # Group by month and team membership
        monthly_stats = (
            df.groupby(["month_year", "is_team_member"])
            .agg(
                {
                    "author": "count",  # PR count
                    "lead_time_days": [
                        "mean",
                        lambda x: x.quantile(0.85),
                        lambda x: x.quantile(0.95),
                    ],
                    "pr_size": "mean",
                }
            )
            .reset_index()
        )

        # Flatten column names
        monthly_stats.columns = pd.Index(
            [
                "month_year",
                "is_team_member",
                "pr_count",
                "avg_lead_time",
                "p85_lead_time",
                "p95_lead_time",
                "avg_pr_size",
            ]
        )

        # Calculate work-days per PR creation rate (using business days)
        monthly_stats["prs_per_work_day"] = monthly_stats.apply(
            lambda row: self._calculate_monthly_work_day_rate(row["month_year"], row["pr_count"]),
            axis=1,
        )

        # Separate team member vs external trends
        team_trends = monthly_stats[monthly_stats["is_team_member"]].copy()
        external_trends = monthly_stats[~monthly_stats["is_team_member"]].copy()

        # Calculate growth trends
        external_growth = self._calculate_growth_trend(external_trends, "pr_count")
        team_growth = self._calculate_growth_trend(team_trends, "pr_count")

        # Convert Period objects to strings for JSON serialization
        monthly_stats["month_year_str"] = monthly_stats["month_year"].astype(str)
        team_trends["month_year_str"] = team_trends["month_year"].astype(str)
        external_trends["month_year_str"] = external_trends["month_year"].astype(str)

        # Drop original Period columns before converting to dict
        monthly_stats_dict = monthly_stats.drop("month_year", axis=1).to_dict("records")
        team_trends_dict = team_trends.drop("month_year", axis=1).to_dict("records")
        external_trends_dict = external_trends.drop("month_year", axis=1).to_dict("records")

        return {
            "monthly_stats": monthly_stats_dict,
            "team_trends": team_trends_dict,
            "external_trends": external_trends_dict,
            "external_pr_growth_rate": external_growth,
            "team_pr_growth_rate": team_growth,
            "total_months_analyzed": len(monthly_stats["month_year"].unique()),
        }

    def _calculate_growth_trend(self, data: pd.DataFrame, metric: str) -> float:
        """Calculate growth trend rate for a metric over time."""
        if len(data) < 2:
            return 0.0

        # Simple linear regression to get growth rate
        x = range(len(data))
        y = data[metric].values

        if len(y) > 1:
            # Manual calculation of linear regression slope
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[i] * y[i] for i in range(n))
            sum_x2 = sum(x[i] * x[i] for i in range(n))

            # Avoid division by zero
            denominator = n * sum_x2 - sum_x * sum_x
            if denominator == 0:
                return 0.0

            slope = (n * sum_xy - sum_x * sum_y) / denominator
            return float(slope)
        return 0.0

    def _calculate_correlations(self, df: pd.DataFrame) -> dict[str, Any]:
        """Calculate correlations between PR metrics and lead time."""
        self.logger.info("Calculating correlation analysis")

        # Select numeric columns for correlation
        correlation_columns = [
            "lead_time_days",
            "pr_size",
            "additions",
            "deletions",
            "changed_files",
            "commits",
            "reviews_count",
            "review_comments",
            "approvals_count",
            "requested_reviewers_count",
        ]

        # Filter to existing columns
        available_columns = [col for col in correlation_columns if col in df.columns]
        corr_df = df[available_columns].corr()

        # Focus on correlations with lead_time_days
        lead_time_correlations = corr_df["lead_time_days"].drop("lead_time_days").to_dict()

        # Find strongest correlations
        strong_correlations = {
            k: v for k, v in lead_time_correlations.items() if abs(v) > 0.3
        }  # Moderate to strong correlation

        return {
            "correlation_matrix": corr_df.to_dict(),
            "lead_time_correlations": lead_time_correlations,
            "strong_correlations": strong_correlations,
            "correlation_summary": self._summarize_correlations(strong_correlations),
        }

    def _summarize_correlations(self, correlations: dict[str, float]) -> list[str]:
        """Generate human-readable correlation summary."""
        summary = []
        for metric, correlation in sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True):
            strength = "strong" if abs(correlation) > 0.6 else "moderate"
            direction = "positive" if correlation > 0 else "negative"
            summary.append(f"{metric}: {strength} {direction} correlation ({correlation:.3f})")
        return summary

    def _analyze_codeowners_pressure(self, df: pd.DataFrame, args) -> dict[str, Any]:
        """Analyze metrics that indicate CODEOWNERS workload pressure including team capacity."""
        self.logger.info("Analyzing CODEOWNERS pressure metrics for external PRs")

        # Get team size from args (with fallback)
        team_size = getattr(args, "team_size", 6)

        # Calculate total PRs and external split
        total_prs = len(df)
        internal_prs = df[df["is_team_member"]]
        external_prs = df[~df["is_team_member"]]

        total_internal_prs = len(internal_prs)
        total_external_prs = len(external_prs)

        if total_external_prs == 0:
            self.logger.warning("No external PRs found in dataset")
            return {"error": "No external PRs found"}

        # Calculate external PR fraction and team impact
        external_pr_fraction = total_external_prs / total_prs if total_prs > 0 else 0
        prs_per_engineer = total_prs / team_size if team_size > 0 else 0
        external_prs_per_engineer = total_external_prs / team_size if team_size > 0 else 0

        # Monthly external PR volume trend - convert Period index to string
        monthly_external_volume_raw = external_prs.groupby("month_year").size()
        monthly_external_volume = {str(k): int(v) for k, v in monthly_external_volume_raw.to_dict().items()}

        # Lead time statistics for external PRs
        external_avg_lead_time = external_prs["lead_time_days"].mean()
        external_median_lead_time = external_prs["lead_time_days"].median()
        external_p95_lead_time = external_prs["lead_time_days"].quantile(0.95)

        # Review workload indicators for external PRs
        avg_reviews_per_pr = external_prs["reviews_count"].mean()
        avg_review_comments_per_pr = external_prs["review_comments"].mean()
        avg_requested_reviewers = external_prs["requested_reviewers_count"].mean()

        # PR size statistics for external PRs
        external_avg_size = external_prs["pr_size"].mean()
        external_median_size = external_prs["pr_size"].median()
        external_max_size = external_prs["pr_size"].max()

        # Time to first review statistics
        if "time_to_first_review_seconds" in external_prs.columns:
            first_review_times = external_prs["time_to_first_review_seconds"] / 3600  # Convert to hours
            avg_time_to_first_review_hours = first_review_times.mean()
            median_time_to_first_review_hours = first_review_times.median()
            p95_time_to_first_review_hours = first_review_times.quantile(0.95)
        else:
            avg_time_to_first_review_hours = 0
            median_time_to_first_review_hours = 0
            p95_time_to_first_review_hours = 0

        # Review rounds and re-work analysis (new GraphQL metrics)
        avg_review_rounds = external_prs.get("review_rounds", pd.Series([0])).mean()
        avg_synchronize_events = external_prs.get("synchronize_after_first_review", pd.Series([0])).mean()
        avg_re_review_pushes = external_prs.get("re_review_pushes", pd.Series([0])).mean()

        # Calculate external PR workload intensity metrics
        workload_intensity = self._calculate_external_workload_intensity(external_prs)

        # Team member comparison (if team PRs exist for comparison)
        team_prs = df[df["is_team_member"]]
        comparison_metrics = self._calculate_team_comparison_metrics(external_prs, team_prs)

        # Calculate pressure score using updated formula
        pressure_score = self._calculate_pressure_score(
            100.0,  # All analyzed PRs are external
            external_avg_lead_time,
            0,  # No team baseline needed since we focus on external only
            avg_reviews_per_pr,
            external_avg_size,
        )

        return {
            # Total PR metrics
            "total_prs": total_prs,
            "total_internal_prs": total_internal_prs,
            "total_external_prs": total_external_prs,
            "external_pr_fraction": external_pr_fraction,
            # Team capacity metrics
            "team_size": team_size,
            "prs_per_engineer": prs_per_engineer,
            "external_prs_per_engineer": external_prs_per_engineer,
            # External PR specific metrics
            "monthly_external_volume_trend": monthly_external_volume,
            "external_avg_lead_time_days": external_avg_lead_time,
            "external_median_lead_time_days": external_median_lead_time,
            "external_p95_lead_time_days": external_p95_lead_time,
            "avg_reviews_per_external_pr": avg_reviews_per_pr,
            "avg_review_comments_per_external_pr": avg_review_comments_per_pr,
            "avg_requested_reviewers_per_pr": avg_requested_reviewers,
            "external_avg_pr_size": external_avg_size,
            "external_median_pr_size": external_median_size,
            "external_max_pr_size": external_max_size,
            "avg_time_to_first_review_hours": avg_time_to_first_review_hours,
            "median_time_to_first_review_hours": median_time_to_first_review_hours,
            "p95_time_to_first_review_hours": p95_time_to_first_review_hours,
            "avg_review_rounds": avg_review_rounds,
            "avg_synchronize_events_after_review": avg_synchronize_events,
            "avg_re_review_pushes": avg_re_review_pushes,
            "workload_intensity_metrics": workload_intensity,
            "team_comparison_metrics": comparison_metrics,
            "pressure_score": pressure_score,
            "pressure_level": self._categorize_pressure_level(pressure_score),
            "pressure_formula": {
                "description": "Enhanced composite pressure score for external PR workload on CODEOWNERS team",
                "formula": "Score = 0.3×(external_ratio/50) + 0.25×(lead_time/10) + 0.25×(reviews/5) + 0.15×(pr_size/1000) + 0.05×(review_rounds/3)",
                "weights": {
                    "external_ratio_weight": 0.3,
                    "lead_time_weight": 0.25,
                    "reviews_burden_weight": 0.25,
                    "pr_size_weight": 0.15,
                    "review_rounds_weight": 0.05,
                },
                "normalization_factors": {
                    "external_ratio_cap": "50% (high pressure threshold)",
                    "lead_time_cap": "10 days (high pressure threshold)",
                    "reviews_cap": "5 reviews per PR (high burden threshold)",
                    "pr_size_cap": "1000 lines (large PR threshold)",
                    "review_rounds_cap": "3 rounds (high rework threshold)",
                },
                "interpretation": {
                    "low_pressure": "0.0-0.4 (manageable workload)",
                    "moderate_pressure": "0.4-0.7 (increased attention needed)",
                    "high_pressure": "0.7-1.0 (critical workload level)",
                },
            },
        }

    def _calculate_external_workload_intensity(self, external_prs: pd.DataFrame) -> dict[str, Any]:
        """Calculate workload intensity metrics specific to external PRs."""
        if len(external_prs) == 0:
            return {}

        # Calculate work-day external PR submission rate (business days only)
        if "created_at" in external_prs.columns:
            min_date = external_prs["created_at"].min()
            max_date = external_prs["created_at"].max()
            business_days_in_range = self._get_date_range_business_days(min_date, max_date)
            calendar_days_range = (max_date - min_date).days

            # Work-day rate (excluding weekends)
            work_day_external_pr_rate = len(external_prs) / max(business_days_in_range, 1)

            # Keep calendar days for reference but focus on work-day calculations
            daily_external_pr_rate = len(external_prs) / max(calendar_days_range, 1)
        else:
            business_days_in_range = 0
            calendar_days_range = 0
            work_day_external_pr_rate = 0
            daily_external_pr_rate = 0

        # Calculate rework intensity (based on review rounds and synchronize events)
        avg_rework_events = external_prs.get("synchronize_after_first_review", pd.Series([0])).mean()
        high_rework_prs = len(external_prs[external_prs.get("review_rounds", pd.Series([0])) >= 2])
        rework_percentage = (high_rework_prs / len(external_prs)) * 100 if len(external_prs) > 0 else 0

        # Calculate response time pressure
        if "time_to_first_review_seconds" in external_prs.columns:
            slow_response_prs = len(external_prs[external_prs["time_to_first_review_seconds"] > 86400])  # > 24h
            slow_response_percentage = (slow_response_prs / len(external_prs)) * 100
        else:
            slow_response_percentage = 0

        # Calculate complex PR burden
        large_prs = len(external_prs[external_prs["pr_size"] > 500])
        complex_prs_percentage = (large_prs / len(external_prs)) * 100 if len(external_prs) > 0 else 0

        return {
            "work_day_external_pr_rate": work_day_external_pr_rate,
            "daily_external_pr_rate": daily_external_pr_rate,  # Keep for reference
            "avg_rework_events_per_pr": avg_rework_events,
            "high_rework_prs_count": high_rework_prs,
            "rework_percentage": rework_percentage,
            "slow_response_prs_percentage": slow_response_percentage,
            "complex_prs_percentage": complex_prs_percentage,
            "total_analysis_business_days": business_days_in_range,
            "total_analysis_calendar_days": calendar_days_range,
        }

    def _calculate_team_comparison_metrics(self, external_prs: pd.DataFrame, team_prs: pd.DataFrame) -> dict[str, Any]:
        """Calculate comparison metrics between external and team PRs."""
        if len(team_prs) == 0:
            return {
                "comparison_available": False,
                "reason": "No team PRs found for comparison",
            }

        comparison = {}

        # Lead time comparison
        ext_lead_time = external_prs["lead_time_days"].mean()
        team_lead_time = team_prs["lead_time_days"].mean()
        comparison["lead_time_difference_days"] = ext_lead_time - team_lead_time
        comparison["lead_time_ratio"] = ext_lead_time / team_lead_time if team_lead_time > 0 else 0

        # Review burden comparison
        ext_reviews = external_prs["reviews_count"].mean()
        team_reviews = team_prs["reviews_count"].mean()
        comparison["review_burden_difference"] = ext_reviews - team_reviews
        comparison["review_burden_ratio"] = ext_reviews / team_reviews if team_reviews > 0 else 0

        # PR size comparison
        ext_size = external_prs["pr_size"].mean()
        team_size = team_prs["pr_size"].mean()
        comparison["size_difference"] = ext_size - team_size
        comparison["size_ratio"] = ext_size / team_size if team_size > 0 else 0

        # Time to first review comparison (if available)
        if (
            "time_to_first_review_seconds" in external_prs.columns
            and "time_to_first_review_seconds" in team_prs.columns
        ):
            ext_response = external_prs["time_to_first_review_seconds"].mean() / 3600  # hours
            team_response = team_prs["time_to_first_review_seconds"].mean() / 3600  # hours
            comparison["response_time_difference_hours"] = ext_response - team_response
            comparison["response_time_ratio"] = ext_response / team_response if team_response > 0 else 0

        # Review rounds comparison (if available)
        if "review_rounds" in external_prs.columns and "review_rounds" in team_prs.columns:
            ext_rounds = external_prs["review_rounds"].mean()
            team_rounds = team_prs["review_rounds"].mean()
            comparison["review_rounds_difference"] = ext_rounds - team_rounds
            comparison["review_rounds_ratio"] = ext_rounds / team_rounds if team_rounds > 0 else 0

        comparison["comparison_available"] = True
        comparison["external_prs_count"] = len(external_prs)
        comparison["team_prs_count"] = len(team_prs)

        return comparison

    def _calculate_pressure_score(
        self,
        external_percentage: float,
        external_lead_time: float,
        team_lead_time: float,
        reviews_per_pr: float,
        external_pr_size: float,
    ) -> float:
        """Calculate an enhanced composite pressure score for CODEOWNERS."""
        # Updated weights to include review rounds impact
        weights = {
            "external_ratio": 0.3,  # Higher external PR ratio = more pressure
            "lead_time_diff": 0.25,  # Higher lead time difference = more pressure
            "reviews_burden": 0.25,  # More reviews per PR = more pressure
            "pr_size": 0.15,  # Larger PRs = more pressure
            "review_rounds": 0.05,  # More review rounds = more rework pressure
        }

        # Normalize external percentage (0-100% -> 0-1)
        external_factor = min(external_percentage / 50, 1.0)  # Cap at 50% as high pressure

        # Normalize lead time difference (assume 10 days difference as high pressure)
        lead_time_factor = min(max(external_lead_time - team_lead_time, 0) / 10, 1.0)

        # Normalize reviews burden (assume 5 reviews per PR as high burden)
        reviews_factor = min(reviews_per_pr / 5, 1.0)

        # Normalize PR size (assume 1000 lines as large PR)
        size_factor = min(external_pr_size / 1000, 1.0)

        # Review rounds factor (assume 3 rounds as high rework)
        # This would need to be passed as parameter or calculated here
        review_rounds_factor = 0  # Placeholder for now

        # Calculate weighted score
        pressure_score = (
            weights["external_ratio"] * external_factor
            + weights["lead_time_diff"] * lead_time_factor
            + weights["reviews_burden"] * reviews_factor
            + weights["pr_size"] * size_factor
            + weights["review_rounds"] * review_rounds_factor
        )

        return pressure_score

    def _categorize_pressure_level(self, pressure_score: float) -> str:
        """Categorize pressure level based on score."""
        if pressure_score >= 0.7:
            return "HIGH"
        elif pressure_score >= 0.4:
            return "MEDIUM"
        else:
            return "LOW"

    def _analyze_team_members_workload(self, df: pd.DataFrame) -> dict[str, Any]:
        """Analyze workload for specific team members from the configured teams.

        Args:
            df: DataFrame with PR data

        Returns:
            Dictionary with team member analysis
        """
        self.logger.info("Analyzing team members workload")

        # Define the specific teams we want to analyze
        target_teams = [
            "@syngenta-digital/cropwise-core-services-catalog",
            "@syngenta-digital/cropwise-core-services-identity",
            "@syngenta-digital/cropwise-core-services-da-backbone",
        ]

        team_analysis = {
            "target_teams": target_teams,
            "pr_counts_by_author": {},
            "team_member_stats": {},
            "external_contributor_stats": {},
            "approver_analysis": {},
            "summary_metrics": {},
        }

        # Analyze PRs by author for team members
        team_member_prs = df[df["is_team_member"] == True].copy()
        external_prs = df[df["is_team_member"] == False].copy()

        # Count PRs by team member authors
        if not team_member_prs.empty:
            pr_counts = team_member_prs["author"].value_counts().to_dict()
            team_analysis["pr_counts_by_author"] = pr_counts

            # Calculate stats for each team member
            for author, count in pr_counts.items():
                author_prs = team_member_prs[team_member_prs["author"] == author]

                team_analysis["team_member_stats"][author] = {
                    "total_prs": count,
                    "avg_lead_time_days": author_prs["lead_time_days"].mean() if len(author_prs) > 0 else 0,
                    "avg_additions": author_prs["additions"].mean()
                    if "additions" in author_prs.columns and len(author_prs) > 0
                    else 0,
                    "avg_deletions": author_prs["deletions"].mean()
                    if "deletions" in author_prs.columns and len(author_prs) > 0
                    else 0,
                    "avg_changed_files": author_prs["changed_files"].mean()
                    if "changed_files" in author_prs.columns and len(author_prs) > 0
                    else 0,
                    "avg_commits": author_prs["commits"].mean()
                    if "commits" in author_prs.columns and len(author_prs) > 0
                    else 0,
                    "avg_reviews": author_prs["reviews_count"].mean()
                    if "reviews_count" in author_prs.columns and len(author_prs) > 0
                    else 0,
                }

        # Analyze external contributors
        if not external_prs.empty:
            ext_pr_counts = external_prs["author"].value_counts().to_dict()

            for author, count in list(ext_pr_counts.items())[:10]:  # Top 10 external contributors
                author_prs = external_prs[external_prs["author"] == author]

                team_analysis["external_contributor_stats"][author] = {
                    "total_prs": count,
                    "avg_lead_time_days": author_prs["lead_time_days"].mean() if len(author_prs) > 0 else 0,
                    "avg_additions": author_prs["additions"].mean()
                    if "additions" in author_prs.columns and len(author_prs) > 0
                    else 0,
                    "avg_changed_files": author_prs["changed_files"].mean()
                    if "changed_files" in author_prs.columns and len(author_prs) > 0
                    else 0,
                }

        # Analyze approvers if data is available
        if "approvers" in df.columns:
            all_approvers = []
            for _, row in df.iterrows():
                if pd.notna(row["approvers"]) and isinstance(row["approvers"], (list, str)):
                    if isinstance(row["approvers"], str):
                        # Handle string representation of list
                        try:
                            import ast

                            approvers = ast.literal_eval(row["approvers"])
                        except:
                            approvers = [row["approvers"]]
                    else:
                        approvers = row["approvers"]

                    all_approvers.extend(approvers)

            if all_approvers:
                from collections import Counter

                approver_counts = Counter(all_approvers)
                team_analysis["approver_analysis"] = {
                    "top_approvers": dict(approver_counts.most_common(10)),
                    "total_unique_approvers": len(set(all_approvers)),
                    "total_approvals": len(all_approvers),
                }

        # Calculate summary metrics
        team_analysis["summary_metrics"] = {
            "total_team_member_prs": len(team_member_prs),
            "total_external_prs": len(external_prs),
            "unique_team_members": len(team_analysis["pr_counts_by_author"]),
            "unique_external_contributors": len(external_prs["author"].unique()) if not external_prs.empty else 0,
            "avg_team_member_lead_time": team_member_prs["lead_time_days"].mean() if not team_member_prs.empty else 0,
            "avg_external_lead_time": external_prs["lead_time_days"].mean() if not external_prs.empty else 0,
            "external_to_team_pr_ratio": len(external_prs) / len(team_member_prs) if len(team_member_prs) > 0 else 0,
        }

        self.logger.info(
            f"Team analysis completed. Found {team_analysis['summary_metrics']['unique_team_members']} team members and {team_analysis['summary_metrics']['unique_external_contributors']} external contributors"
        )

        return team_analysis

    def _generate_insights(
        self,
        df: pd.DataFrame,
        monthly_trends: dict[str, Any],
        correlations: dict[str, Any],
        pressure_metrics: dict[str, Any],
        team_analysis: dict[str, Any],
    ) -> dict[str, list[str]]:
        """Generate insights and recommendations from the analysis."""
        self.logger.info("Generating insights and recommendations")

        key_points = []
        recommendations = []

        # Analyze external PR trends
        external_growth = monthly_trends.get("external_pr_growth_rate", 0)
        if external_growth > 1:
            key_points.append(f"External PR submissions are increasing at {external_growth:.1f} PRs per month")
            recommendations.append("Monitor external PR growth trend and consider scaling review capacity")

        # Analyze pressure level
        pressure_level = pressure_metrics.get("pressure_level", "UNKNOWN")
        pressure_score = pressure_metrics.get("pressure_score", 0)
        key_points.append(f"CODEOWNERS pressure level: {pressure_level} (score: {pressure_score:.2f})")

        if pressure_level == "HIGH":
            recommendations.extend(
                [
                    "Consider adding more CODEOWNERS or distributing review load",
                    "Implement automated checks to reduce manual review burden",
                    "Set up PR templates and guidelines for external contributors",
                ]
            )

        # Analyze lead time differences
        lead_time_diff = pressure_metrics.get("lead_time_difference", 0)
        if lead_time_diff > 2:
            key_points.append(f"External PRs take {lead_time_diff:.1f} days longer to merge than team PRs")
            recommendations.append("Investigate bottlenecks in external PR review process")

        # Analyze PR size impact
        strong_correlations = correlations.get("strong_correlations", {})
        if "pr_size" in strong_correlations:
            correlation_value = strong_correlations["pr_size"]
            if correlation_value > 0.5:
                key_points.append(f"PR size strongly correlates with lead time (r={correlation_value:.3f})")
                recommendations.extend(
                    [
                        "Encourage smaller, focused PRs through contributor guidelines",
                        "Consider automated PR size warnings or requirements",
                    ]
                )

        # Analyze external PR percentage
        external_percentage = pressure_metrics.get("external_prs_percentage", 0)
        if external_percentage > 40:
            key_points.append(f"External authors contribute {external_percentage:.1f}% of all PRs")
            recommendations.append("Evaluate if current CODEOWNERS capacity matches external contribution volume")

        return {"key_points": key_points, "recommendations": recommendations}

    def _ensure_output_directory(self, output_path: str) -> str:
        """Ensure output directory exists with timestamped subfolder and return absolute path."""
        # Create timestamped directory name (using minutes instead of seconds for uniqueness per execution)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        timestamped_dirname = f"pr-workload-analysis_{timestamp}"

        # Create full path: output_path/pr-workload-analysis_YYYYMMDD_HHMM/
        output_dir = os.path.abspath(os.path.join(output_path, timestamped_dirname))

        # If directory already exists (multiple runs in same minute), add a counter
        counter = 1
        final_output_dir = output_dir
        while os.path.exists(final_output_dir):
            final_timestamped_dirname = f"{timestamped_dirname}_{counter:02d}"
            final_output_dir = os.path.abspath(os.path.join(output_path, final_timestamped_dirname))
            counter += 1
            # Safety limit to avoid infinite loop
            if counter > 99:
                break

        FileManager.create_folder(final_output_dir)

        final_dirname = os.path.basename(final_output_dir)
        self.logger.info(f"Created timestamped output directory: {final_dirname}")
        return final_output_dir

    def _save_results(
        self,
        df: pd.DataFrame,
        monthly_trends: dict[str, Any],
        correlations: dict[str, Any],
        pressure_metrics: dict[str, Any],
        insights: dict[str, list[str]],
        output_dir: str,
        detailed: bool,
    ) -> list[str]:
        """Save analysis results to files."""
        self.logger.info(f"Saving results to: {output_dir}")
        output_files = []

        # 1. Complete summary JSON
        summary_file = os.path.join(output_dir, "pr_workload_summary.json")
        summary_data = {
            "analysis_metadata": {
                "total_prs": len(df),
                "analysis_period": self._get_analysis_period(df),
                "generated_at": datetime.now().isoformat(),
            },
            "monthly_trends": monthly_trends,
            "correlations": correlations,
            "pressure_metrics": pressure_metrics,
            "insights": insights,
        }
        # Convert all pandas/numpy types to JSON-serializable types
        summary_data_serializable = self._convert_to_json_serializable(summary_data)
        JSONManager.write_json(summary_data_serializable, summary_file)
        output_files.append(summary_file)

        # 2. Monthly trends CSV
        trends_file = os.path.join(output_dir, "monthly_trends.csv")
        trends_df = pd.DataFrame(monthly_trends["monthly_stats"])
        trends_df.to_csv(trends_file, index=False)
        output_files.append(trends_file)

        # 3. Correlation matrix CSV
        corr_file = os.path.join(output_dir, "correlation_matrix.csv")
        corr_df = pd.DataFrame(correlations["correlation_matrix"])
        corr_df.to_csv(corr_file)
        output_files.append(corr_file)

        # 4. CODEOWNERS pressure metrics CSV
        pressure_file = os.path.join(output_dir, "codeowners_pressure_metrics.csv")
        pressure_df = pd.DataFrame([pressure_metrics])
        pressure_df.to_csv(pressure_file, index=False)
        output_files.append(pressure_file)

        # 5. Recommendations text file
        recommendations_file = os.path.join(output_dir, "recommendations.txt")
        with open(recommendations_file, "w") as f:
            f.write("PR Workload Analysis - Insights and Recommendations\n")
            f.write("=" * 60 + "\n\n")

            f.write("Key Insights:\n")
            for i, insight in enumerate(insights["key_points"], 1):
                f.write(f"{i}. {insight}\n")

            f.write("\nRecommendations:\n")
            for i, recommendation in enumerate(insights["recommendations"], 1):
                f.write(f"{i}. {recommendation}\n")

            f.write(f"\nAnalysis generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        output_files.append(recommendations_file)

        # 6. Detailed data if requested
        if detailed:
            detailed_file = os.path.join(output_dir, "detailed_pr_data.csv")
            df.to_csv(detailed_file, index=False)
            output_files.append(detailed_file)

        return output_files

    def _generate_charts(
        self,
        df: pd.DataFrame,
        monthly_trends: dict[str, Any],
        correlations: dict[str, Any],
        output_dir: str,
    ) -> list[str]:
        """Generate visualization charts."""
        self.logger.info("Generating visualization charts")

        charts_dir = os.path.join(output_dir, "charts")
        FileManager.create_folder(charts_dir)
        chart_files = []

        try:
            # Set style
            plt.style.use("default")

            # 1. Monthly external PR volume chart
            trend_chart = os.path.join(charts_dir, "monthly_external_pr_volume.png")
            self.logger.info(f"Creating monthly external PR volume chart: {trend_chart}")
            self._create_monthly_external_volume_chart(df, trend_chart)
            chart_files.append(trend_chart)

            # 2. Lead time distribution chart (external PRs only)
            leadtime_chart = os.path.join(charts_dir, "external_lead_time_distribution.png")
            self.logger.info(f"Creating external lead time chart: {leadtime_chart}")
            self._create_lead_time_chart(df, leadtime_chart)
            chart_files.append(leadtime_chart)

            # 3. Correlation heatmap
            corr_chart = os.path.join(charts_dir, "correlation_heatmap.png")
            self.logger.info(f"Creating correlation heatmap: {corr_chart}")
            self._create_correlation_heatmap(correlations, corr_chart)
            chart_files.append(corr_chart)

            # 4. PR size vs lead time scatter (external PRs only)
            scatter_chart = os.path.join(charts_dir, "external_pr_size_vs_lead_time.png")
            self.logger.info(f"Creating external PR scatter chart: {scatter_chart}")
            self._create_scatter_chart(df, scatter_chart)
            chart_files.append(scatter_chart)

            # 5. Monthly PR volume histogram
            histogram_chart = os.path.join(charts_dir, "monthly_pr_volume_histogram.png")
            self.logger.info(f"Creating monthly volume histogram: {histogram_chart}")
            self._create_monthly_histogram(df, histogram_chart)
            chart_files.append(histogram_chart)

            # 6. Work-day volume chart for periods <= 3 months
            period_months = self._calculate_analysis_period_months(df)
            if period_months <= 3:
                work_day_chart = os.path.join(charts_dir, "work_day_volume_chart.png")
                self.logger.info(
                    f"Creating work-day volume chart (period: {period_months:.1f} months): {work_day_chart}"
                )
                self._create_work_day_volume_chart(df, work_day_chart)
                chart_files.append(work_day_chart)
            else:
                self.logger.info(f"Skipping work-day volume chart (period: {period_months:.1f} months > 3 months)")

        except Exception as e:
            self.logger.warning(f"Failed to generate some charts: {e}")

        return chart_files

    def _create_monthly_trends_chart(self, monthly_trends: dict[str, Any], output_path: str):
        """Create monthly PR trends chart."""
        self.logger.info(f"Starting monthly trends chart creation: {output_path}")
        try:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

            # Get data
            team_trends = monthly_trends.get("team_trends", [])
            external_trends = monthly_trends.get("external_trends", [])

            team_df = None
            external_df = None

            if team_trends:
                team_df = pd.DataFrame(team_trends)
                team_df["month_year"] = pd.to_datetime(team_df["month_year"].astype(str))
                ax1.plot(
                    team_df["month_year"],
                    team_df["pr_count"],
                    marker="o",
                    label="Team Members",
                    linewidth=2,
                )

            if external_trends:
                external_df = pd.DataFrame(external_trends)
                external_df["month_year"] = pd.to_datetime(external_df["month_year"].astype(str))
                ax1.plot(
                    external_df["month_year"],
                    external_df["pr_count"],
                    marker="s",
                    label="External Contributors",
                    linewidth=2,
                )

            ax1.set_title("Monthly PR Count Trends", fontsize=14, fontweight="bold")
            ax1.set_xlabel("Month")
            ax1.set_ylabel("Number of PRs")
            ax1.legend()
            ax1.grid(True, alpha=0.3)

            # Lead time trends
            if team_df is not None:
                ax2.plot(
                    team_df["month_year"],
                    team_df["avg_lead_time"],
                    marker="o",
                    label="Team Members",
                    linewidth=2,
                )

            if external_df is not None:
                ax2.plot(
                    external_df["month_year"],
                    external_df["avg_lead_time"],
                    marker="s",
                    label="External Contributors",
                    linewidth=2,
                )

            ax2.set_title("Monthly Average Lead Time Trends", fontsize=14, fontweight="bold")
            ax2.set_xlabel("Month")
            ax2.set_ylabel("Average Lead Time (days)")
            ax2.legend()
            ax2.grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Monthly trends chart saved: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to create monthly trends chart: {e}")
            # Don't raise, continue with other charts

    def _create_lead_time_chart(self, df: pd.DataFrame, output_path: str):
        """Create lead time distribution chart."""
        self.logger.info(f"Creating lead time chart: {output_path}")
        try:
            fig, ax = plt.subplots(figsize=(10, 6))

            team_prs = df[df["is_team_member"]]["lead_time_days"]
            external_prs = df[~df["is_team_member"]]["lead_time_days"]

            ax.hist(team_prs, bins=30, alpha=0.7, label="Team Members", density=True)
            ax.hist(
                external_prs,
                bins=30,
                alpha=0.7,
                label="External Contributors",
                density=True,
            )

            ax.set_title("Lead Time Distribution Comparison", fontsize=14, fontweight="bold")
            ax.set_xlabel("Lead Time (days)")
            ax.set_ylabel("Density")
            ax.legend()
            ax.grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Lead time chart saved: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to create lead time chart: {e}")

    def _create_correlation_heatmap(self, correlations: dict[str, Any], output_path: str):
        """Create correlation heatmap."""
        self.logger.info(f"Creating correlation heatmap: {output_path}")
        try:
            corr_matrix = correlations.get("correlation_matrix", {})
            if not corr_matrix:
                self.logger.warning("No correlation matrix data available")
                return

            fig, ax = plt.subplots(figsize=(10, 8))
            corr_df = pd.DataFrame(corr_matrix)

            # Create a simple heatmap using matplotlib
            im = ax.imshow(corr_df.values, cmap="RdBu_r", aspect="auto", vmin=-1, vmax=1)

            # Set ticks and labels
            ax.set_xticks(range(len(corr_df.columns)))
            ax.set_yticks(range(len(corr_df.index)))
            ax.set_xticklabels(corr_df.columns, rotation=45, ha="right")
            ax.set_yticklabels(corr_df.index)

            # Add text annotations
            for i in range(len(corr_df.index)):
                for j in range(len(corr_df.columns)):
                    ax.text(
                        j,
                        i,
                        f"{corr_df.iloc[i, j]:.2f}",
                        ha="center",
                        va="center",
                        color="black",
                    )

            # Add colorbar
            plt.colorbar(im, ax=ax, shrink=0.8)

            ax.set_title(
                "Correlation Matrix - PR Metrics vs Lead Time",
                fontsize=14,
                fontweight="bold",
            )
            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Correlation heatmap saved: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to create correlation heatmap: {e}")

    def _create_scatter_chart(self, df: pd.DataFrame, output_path: str):
        """Create PR size vs lead time scatter chart."""
        self.logger.info(f"Creating scatter chart: {output_path}")
        try:
            fig, ax = plt.subplots(figsize=(10, 6))

            team_prs = df[df["is_team_member"]]
            external_prs = df[~df["is_team_member"]]

            ax.scatter(
                team_prs["pr_size"],
                team_prs["lead_time_days"],
                alpha=0.6,
                label="Team Members",
                s=30,
            )
            ax.scatter(
                external_prs["pr_size"],
                external_prs["lead_time_days"],
                alpha=0.6,
                label="External Contributors",
                s=30,
            )

            ax.set_title("PR Size vs Lead Time", fontsize=14, fontweight="bold")
            ax.set_xlabel("PR Size (additions + deletions)")
            ax.set_ylabel("Lead Time (days)")
            ax.legend()
            ax.grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Scatter chart saved: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to create scatter chart: {e}")

    def _create_monthly_external_volume_chart(self, df: pd.DataFrame, output_path: str):
        """Create monthly external PR volume chart focusing only on external PRs."""
        self.logger.info(f"Creating monthly external PR volume chart: {output_path}")
        try:
            # Filter external PRs only
            external_prs = df[~df["is_team_member"]]

            if len(external_prs) == 0:
                self.logger.warning("No external PRs found for chart")
                return

            # Group by month
            monthly_counts = external_prs.groupby("month_year").size()
            monthly_avg_lead_time = external_prs.groupby("month_year")["lead_time_days"].mean()

            # Convert Period index to datetime for plotting
            dates = [pd.to_datetime(str(period)) for period in monthly_counts.index]

            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

            # PR Volume
            ax1.bar(
                dates,
                monthly_counts.values,
                color="steelblue",
                alpha=0.7,
                edgecolor="navy",
            )
            ax1.set_title("Monthly External PR Volume", fontsize=16, fontweight="bold", pad=20)
            ax1.set_ylabel("Number of External PRs", fontsize=12)
            ax1.grid(True, alpha=0.3)

            # Add value labels on bars
            for i, (date, count) in enumerate(zip(dates, monthly_counts.values)):
                ax1.text(date, count + 0.5, str(count), ha="center", va="bottom", fontsize=10)

            # Average Lead Time
            ax2.plot(
                dates,
                monthly_avg_lead_time.values,
                marker="o",
                linewidth=3,
                markersize=8,
                color="crimson",
                label="Avg Lead Time",
            )
            ax2.fill_between(dates, monthly_avg_lead_time.values, alpha=0.3, color="crimson")
            ax2.set_title(
                "Monthly Average Lead Time (External PRs)",
                fontsize=16,
                fontweight="bold",
                pad=20,
            )
            ax2.set_xlabel("Month", fontsize=12)
            ax2.set_ylabel("Average Lead Time (days)", fontsize=12)
            ax2.grid(True, alpha=0.3)
            ax2.legend()

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Monthly external volume chart saved: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to create monthly external volume chart: {e}")

    def _create_monthly_histogram(self, df: pd.DataFrame, output_path: str):
        """Create histogram showing distribution of monthly PR volumes."""
        self.logger.info(f"Creating monthly PR volume histogram: {output_path}")
        try:
            # Filter external PRs only
            external_prs = df[~df["is_team_member"]]

            if len(external_prs) == 0:
                self.logger.warning("No external PRs found for histogram")
                return

            # Group by month to get volume distribution
            monthly_counts = external_prs.groupby("month_year").size()

            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

            # Histogram of monthly volumes
            ax1.hist(
                monthly_counts.values,
                bins=min(10, len(monthly_counts)),
                color="lightblue",
                edgecolor="navy",
                alpha=0.7,
            )
            ax1.set_title(
                "Distribution of Monthly External PR Volumes",
                fontsize=14,
                fontweight="bold",
            )
            ax1.set_xlabel("Number of PRs per Month")
            ax1.set_ylabel("Frequency (Number of Months)")
            ax1.grid(True, alpha=0.3)

            # Add stats text
            mean_vol = monthly_counts.mean()
            median_vol = monthly_counts.median()
            ax1.axvline(
                mean_vol,
                color="red",
                linestyle="--",
                linewidth=2,
                label=f"Mean: {mean_vol:.1f}",
            )
            ax1.axvline(
                median_vol,
                color="green",
                linestyle="--",
                linewidth=2,
                label=f"Median: {median_vol:.1f}",
            )
            ax1.legend()

            # PR Size distribution
            ax2.hist(
                external_prs["pr_size"],
                bins=30,
                color="lightcoral",
                edgecolor="darkred",
                alpha=0.7,
            )
            ax2.set_title("Distribution of External PR Sizes", fontsize=14, fontweight="bold")
            ax2.set_xlabel("PR Size (lines changed)")
            ax2.set_ylabel("Frequency (Number of PRs)")
            ax2.grid(True, alpha=0.3)

            # Add percentiles
            p50 = external_prs["pr_size"].quantile(0.5)
            p95 = external_prs["pr_size"].quantile(0.95)
            ax2.axvline(
                p50,
                color="orange",
                linestyle="--",
                linewidth=2,
                label=f"50th percentile: {p50:.0f}",
            )
            ax2.axvline(
                p95,
                color="red",
                linestyle="--",
                linewidth=2,
                label=f"95th percentile: {p95:.0f}",
            )
            ax2.legend()

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Monthly histogram saved: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to create monthly histogram: {e}")

    def _create_work_day_volume_chart(self, df: pd.DataFrame, output_path: str):
        """Create work-day volume chart for periods <= 3 months."""
        self.logger.info(f"Creating work-day volume chart: {output_path}")
        try:
            # Filter external PRs only
            external_prs = df[~df["is_team_member"]]

            if len(external_prs) == 0:
                self.logger.warning("No external PRs found for work-day volume chart")
                return

            # Get date range for analysis
            min_date = external_prs["created_at"].min()
            max_date = external_prs["created_at"].max()

            # Create business day range
            business_days = pd.bdate_range(start=min_date.date(), end=max_date.date())

            # Count PRs per work day
            external_prs_copy = external_prs.copy()
            external_prs_copy["date_only"] = external_prs_copy["created_at"].dt.date
            daily_counts = external_prs_copy.groupby("date_only").size()

            # Create complete series with 0 for days with no PRs (business days only)
            work_day_counts = pd.Series(0, index=business_days.date)
            work_day_counts.update(daily_counts)

            # Create the chart
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10))

            # Top chart: Daily volume bars
            dates = work_day_counts.index
            values = work_day_counts.values

            ax1.bar(dates, values, color="steelblue", alpha=0.7, edgecolor="navy", width=0.8)
            ax1.set_title(
                "Daily External PR Volume (Work Days Only)",
                fontsize=16,
                fontweight="bold",
                pad=20,
            )
            ax1.set_ylabel("Number of PRs", fontsize=12)
            ax1.grid(True, alpha=0.3)

            # Add average line
            avg_daily = work_day_counts.mean()
            ax1.axhline(
                avg_daily,
                color="red",
                linestyle="--",
                linewidth=2,
                label=f"Average: {avg_daily:.1f} PRs/work day",
            )
            ax1.legend()

            # Rotate x-axis labels for better readability
            ax1.tick_params(axis="x", rotation=45)

            # Bottom chart: 7-day rolling average
            rolling_avg = work_day_counts.rolling(window=5, center=True).mean()  # 5-day rolling for work days
            ax2.plot(
                dates,
                rolling_avg,
                color="crimson",
                linewidth=3,
                marker="o",
                markersize=4,
            )
            ax2.fill_between(dates, rolling_avg, alpha=0.3, color="crimson")
            ax2.set_title("5-Work Day Rolling Average", fontsize=16, fontweight="bold", pad=20)
            ax2.set_xlabel("Date", fontsize=12)
            ax2.set_ylabel("Average PRs per Work Day", fontsize=12)
            ax2.grid(True, alpha=0.3)
            ax2.tick_params(axis="x", rotation=45)

            # Add statistics
            stats_text = f"Period: {len(business_days)} work days\\n"
            stats_text += f"Total PRs: {len(external_prs)}\\n"
            stats_text += f"Avg: {avg_daily:.1f} PRs/day\\n"
            stats_text += f"Peak: {work_day_counts.max()} PRs/day"

            ax2.text(
                0.02,
                0.98,
                stats_text,
                transform=ax2.transAxes,
                verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
            )

            plt.tight_layout()
            plt.savefig(output_path, dpi=300, bbox_inches="tight")
            plt.close()
            self.logger.info(f"Work-day volume chart saved: {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to create work-day volume chart: {e}")

    def _generate_markdown_report(
        self,
        df: pd.DataFrame,
        monthly_trends: dict[str, Any],
        correlations: dict[str, Any],
        pressure_metrics: dict[str, Any],
        insights: dict[str, list[str]],
        output_dir: str,
        include_charts: bool = False,
    ) -> str:
        """Generate comprehensive Markdown report."""
        self.logger.info("Generating Markdown report")

        report_file = os.path.join(output_dir, "pr_workload_analysis_report.md")

        # Get analysis metadata
        total_prs = len(df)
        external_prs = len(df[~df["is_team_member"]])
        analysis_period = self._get_analysis_period(df)

        # Calculate key metrics
        pressure_level = pressure_metrics.get("pressure_level", "UNKNOWN")
        pressure_score = pressure_metrics.get("pressure_score", 0)

        with open(report_file, "w", encoding="utf-8") as f:
            # Header
            f.write("# PR Workload Analysis Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Analysis Period:** {analysis_period}\n")
            f.write(f"**Total PRs Analyzed:** {total_prs}\n\n")

            # Executive Summary
            f.write("## 📊 Executive Summary\n\n")

            # Get additional metrics
            total_prs = pressure_metrics.get("total_prs", 0)
            total_internal_prs = pressure_metrics.get("total_internal_prs", 0)
            external_fraction = pressure_metrics.get("external_pr_fraction", 0)
            team_size = pressure_metrics.get("team_size", 6)
            prs_per_engineer = pressure_metrics.get("prs_per_engineer", 0)
            external_prs_per_engineer = pressure_metrics.get("external_prs_per_engineer", 0)

            f.write("| Metric | Value |\n")
            f.write("|--------|-------|\n")
            f.write(f"| **Total PRs Analyzed** | **{total_prs}** |\n")
            f.write(f"| Internal PRs (Team Members) | {total_internal_prs} ({(1 - external_fraction) * 100:.1f}%) |\n")
            f.write(f"| External PRs (Contributors) | {external_prs} ({external_fraction * 100:.1f}%) |\n")
            f.write(f"| Team Size (Reviewers) | {team_size} engineers |\n")
            f.write(f"| PRs per Engineer | {prs_per_engineer:.1f} total |\n")
            f.write(f"| External PRs per Engineer | {external_prs_per_engineer:.1f} |\n")
            f.write(f"| Analysis Period | {analysis_period} |\n")
            f.write(f"| **CODEOWNERS Pressure Level** | **{pressure_level}** |\n")
            f.write(f"| **Pressure Score** | **{pressure_score:.2f}/1.00** |\n\n")

            # Pressure Level Alert
            if pressure_level == "HIGH":
                f.write("🚨 **ALERT: HIGH PRESSURE DETECTED**\n")
                f.write("CODEOWNERS are under significant pressure. Immediate action recommended.\n\n")
            elif pressure_level == "MEDIUM":
                f.write("⚠️ **WARNING: MEDIUM PRESSURE**\n")
                f.write("CODEOWNERS workload is elevated. Monitor closely and consider preventive measures.\n\n")
            else:
                f.write("✅ **PRESSURE LEVEL: LOW**\n")
                f.write("CODEOWNERS workload appears manageable.\n\n")

            # Key Insights
            f.write("## 🔍 Key Insights\n\n")
            for i, insight in enumerate(insights["key_points"], 1):
                f.write(f"{i}. {insight}\n")
            f.write("\n")

            # Monthly Trends Analysis
            f.write("## 📈 Monthly Trends Analysis\n\n")
            self._write_monthly_trends_section(f, monthly_trends, df)

            # Lead Time Analysis
            f.write("## ⏱️ Lead Time Analysis\n\n")
            self._write_lead_time_section(f, df, pressure_metrics)

            # Correlation Analysis
            f.write("## 🔗 Correlation Analysis\n\n")
            self._write_correlation_section(f, correlations)

            # PR Size Impact
            f.write("## 📏 PR Size Impact Analysis\n\n")
            self._write_pr_size_section(f, df, correlations)

            # CODEOWNERS Workload Metrics
            f.write("## 👥 CODEOWNERS Workload Metrics\n\n")
            self._write_codeowners_metrics_section(f, pressure_metrics)

            # Metrics Explanation Section
            self._write_metrics_explanation_section(f, pressure_metrics, correlations)

            # Correlation Analysis Explanation
            self._write_correlation_explanation_section(f, correlations)

            # Recommendations
            f.write("## 💡 Recommendations\n\n")
            for i, recommendation in enumerate(insights["recommendations"], 1):
                f.write(f"{i}. {recommendation}\n")
            f.write("\n")

            # Charts section
            if include_charts:
                f.write("## 📊 Visualizations\n\n")
                charts = [
                    (
                        "charts/external_lead_time_distribution.png",
                        "External PR Lead Time Distribution",
                    ),
                    ("charts/correlation_heatmap.png", "Correlation Matrix"),
                    (
                        "charts/external_pr_size_vs_lead_time.png",
                        "External PR Size vs Lead Time",
                    ),
                    (
                        "charts/monthly_external_pr_volume.png",
                        "Monthly External PR Volume",
                    ),
                    (
                        "charts/monthly_pr_volume_histogram.png",
                        "Monthly Volume Distribution",
                    ),
                    ("charts/work_day_volume_chart.png", "Daily Work-Day Volume Chart"),
                ]

                for chart_path, title in charts:
                    if os.path.exists(os.path.join(output_dir, chart_path)):
                        f.write(f"### {title}\n\n")
                        f.write(f"![{title}]({chart_path})\n\n")

            # Action Items
            f.write("## 🎯 Immediate Action Items\n\n")
            if pressure_level == "HIGH":
                f.write("- [ ] **URGENT**: Scale CODEOWNERS team or redistribute review load\n")
                f.write("- [ ] Implement automated pre-review checks\n")
                f.write("- [ ] Set up external contributor guidelines\n")
                f.write("- [ ] Consider dedicated review time slots\n")
            elif pressure_level == "MEDIUM":
                f.write("- [ ] Monitor pressure metrics weekly\n")
                f.write("- [ ] Review CODEOWNERS capacity planning\n")
                f.write("- [ ] Optimize review processes\n")
            else:
                f.write("- [ ] Continue monitoring monthly\n")
                f.write("- [ ] Maintain current review processes\n")

            f.write("\n---\n")
            f.write("*Report generated by PyToolkit PR Workload Analysis*\n")

        return report_file

    def _write_monthly_trends_section(self, f, monthly_trends: dict[str, Any], df: pd.DataFrame):
        """Write monthly trends section to markdown file."""
        f.write("### External PR Volume Trends\n\n")

        # Get monthly external PR data
        external_df = df[~df["is_team_member"]]
        if len(external_df) == 0:
            f.write("No external PRs found for trend analysis.\n\n")
            return

        monthly_external = (
            external_df.groupby("month_year").agg({"author": "count", "lead_time_days": "mean"}).reset_index()
        )

        if len(monthly_external) > 0:
            f.write("| Month | External PRs | Avg Lead Time (days) | PRs per Work Day |\n")
            f.write("|-------|-------------|---------------------|------------------|\n")

            # Convert Period to string for display and calculate work-day rates
            for _, row in monthly_external.iterrows():
                month_str = str(row["month_year"])
                pr_count = int(row["author"])
                avg_lead_time = row["lead_time_days"]

                # Calculate work-day rate for this month
                work_day_rate = self._calculate_monthly_work_day_rate(row["month_year"], pr_count)

                f.write(f"| {month_str} | {pr_count} | {avg_lead_time:.1f} | {work_day_rate:.2f} |\n")

        # Growth analysis
        external_growth = monthly_trends.get("external_pr_growth_rate", 0)
        f.write("\n**Growth Analysis:**\n")
        f.write(f"- External PR Growth Rate: {external_growth:.1f}% per month\n")
        f.write(f"- Total Months Analyzed: {monthly_trends.get('total_months_analyzed', 0)}\n\n")

    def _write_lead_time_section(self, f, df: pd.DataFrame, pressure_metrics: dict[str, Any]):
        """Write lead time analysis section to markdown file."""
        # Overall statistics focusing on external PRs only
        external_df = df[~df["is_team_member"]]

        if len(external_df) == 0:
            f.write("### Lead Time Statistics\n\n")
            f.write("No external PRs found for analysis.\n\n")
            return

        f.write("### Lead Time Statistics\n\n")
        f.write("| Metric | External PRs |\n")
        f.write("|--------|-------------|\n")

        ext_mean = external_df["lead_time_days"].mean()
        ext_median = external_df["lead_time_days"].median()

        f.write(f"| Mean (days) | {ext_mean:.1f} |\n")
        f.write(f"| Median (days) | {ext_median:.1f} |\n")

        # Percentiles
        f.write("\n**Lead Time Percentiles (External PRs):**\n")
        for percentile in [50, 75, 90, 95]:
            value = external_df["lead_time_days"].quantile(percentile / 100)
            f.write(f"- P{percentile}: {value:.1f} days\n")
        f.write("\n")

    def _write_correlation_section(self, f, correlations: dict[str, Any]):
        """Write correlation analysis section to markdown file."""
        f.write("### Correlation Analysis\n\n")

        # Get lead time correlations
        lead_time_correlations = correlations.get("lead_time_correlations", {})

        f.write("| Metric | Correlation with Lead Time | Interpretation |\n")
        f.write("|--------|----------------------------|----------------|\n")

        # Key correlations to highlight
        key_metrics = {
            "pr_size": "PR Size",
            "additions": "Lines Added",
            "deletions": "Lines Deleted",
            "changed_files": "Files Changed",
            "commits": "Number of Commits",
            "reviews_count": "Review Count",
            "review_comments": "Review Comments",
            "requested_reviewers_count": "Requested Reviewers",
        }

        for metric_key, metric_name in key_metrics.items():
            corr_value = lead_time_correlations.get(metric_key, 0)
            interpretation = self._interpret_correlation(corr_value)
            f.write(f"| {metric_name} | {corr_value:.3f} | {interpretation} |\n")

        # Strong correlations summary
        strong_correlations = correlations.get("strong_correlations", {})
        if strong_correlations:
            f.write("\n**Strong Correlations Found:**\n")
            for metric, corr_value in strong_correlations.items():
                f.write(f"- {metric}: {corr_value:.3f} ({self._interpret_correlation(corr_value)})\n")
        else:
            f.write("\n**No strong correlations (|r| > 0.3) found between metrics and lead time.**\n")

        f.write("\n**Pressure Calculation Formula:**\n")
        pressure_formula = correlations.get("pressure_formula")
        if pressure_formula:
            f.write(f"\n**{pressure_formula['description']}**\n\n")
            f.write(f"**Formula:** `{pressure_formula['formula']}`\n\n")
            f.write("**Factor Weights:**\n")
            weights = pressure_formula["weights"]
            for factor, weight in weights.items():
                f.write(f"- {factor.replace('_', ' ').title()}: {weight * 100:.0f}%\n")

            f.write("\n**Normalization Factors:**\n")
            norm_factors = pressure_formula["normalization_factors"]
            for factor, description in norm_factors.items():
                f.write(f"- {factor.replace('_', ' ').title()}: {description}\n")

            if "interpretation" in pressure_formula:
                f.write("\n**Pressure Level Interpretation:**\n")
                interp = pressure_formula["interpretation"]
                for level, description in interp.items():
                    f.write(f"- {level.replace('_', ' ').title()}: {description}\n")

        f.write("\n")

    def _write_pr_size_section(self, f, df: pd.DataFrame, correlations: dict[str, Any]):
        """Write PR size impact section to markdown file."""
        f.write("### PR Size Distribution\n\n")

        # Size categories
        small_prs = len(df[df["pr_size"] <= 100])
        medium_prs = len(df[(df["pr_size"] > 100) & (df["pr_size"] <= 500)])
        large_prs = len(df[df["pr_size"] > 500])
        total = len(df)

        f.write("| Size Category | Count | Percentage | Avg Lead Time (days) |\n")
        f.write("|---------------|--------|------------|------------------------|\n")

        small_df = df[df["pr_size"] <= 100]
        medium_df = df[(df["pr_size"] > 100) & (df["pr_size"] <= 500)]
        large_df = df[df["pr_size"] > 500]

        small_lead = small_df["lead_time_days"].mean() if len(small_df) > 0 else 0
        medium_lead = medium_df["lead_time_days"].mean() if len(medium_df) > 0 else 0
        large_lead = large_df["lead_time_days"].mean() if len(large_df) > 0 else 0

        f.write(f"| Small (≤100 changes) | {small_prs} | {small_prs / total * 100:.1f}% | {small_lead:.1f} |\n")
        f.write(f"| Medium (101-500) | {medium_prs} | {medium_prs / total * 100:.1f}% | {medium_lead:.1f} |\n")
        f.write(f"| Large (>500) | {large_prs} | {large_prs / total * 100:.1f}% | {large_lead:.1f} |\n")

        # Size impact analysis
        lead_time_correlations = correlations.get("lead_time_correlations", {})
        size_corr = lead_time_correlations.get("pr_size", 0)
        f.write("\n**Size Impact Analysis:**\n")
        f.write(f"- Size-Lead Time Correlation: {size_corr:.3f}\n")

        if abs(size_corr) > 0.3:
            impact = "HIGH IMPACT" if size_corr > 0 else "HIGH NEGATIVE IMPACT"
            f.write(
                f"- **{impact}**: PR size {'significantly increases' if size_corr > 0 else 'significantly decreases'} lead time\n"
            )
        elif abs(size_corr) > 0.1:
            impact = "MODERATE IMPACT" if size_corr > 0 else "MODERATE NEGATIVE IMPACT"
            f.write(
                f"- **{impact}**: PR size has {'noticeable positive' if size_corr > 0 else 'noticeable negative'} effect on lead time\n"
            )
        else:
            f.write("- **LOW IMPACT**: PR size has minimal effect on lead time\n")
        f.write("\n")

    def _write_codeowners_metrics_section(self, f, pressure_metrics: dict[str, Any]):
        """Write CODEOWNERS metrics section to markdown file."""
        f.write("### External PR Workload Metrics\n\n")

        total_prs = pressure_metrics.get("total_external_prs", 0)
        avg_lead_time = pressure_metrics.get("external_avg_lead_time_days", 0)
        avg_reviews = pressure_metrics.get("avg_reviews_per_external_pr", 0)
        avg_size = pressure_metrics.get("external_avg_pr_size", 0)
        score = pressure_metrics.get("pressure_score", 0)
        level = pressure_metrics.get("pressure_level", "UNKNOWN")

        # Get work-day metrics from workload intensity
        workload_metrics = pressure_metrics.get("workload_intensity_metrics", {})
        work_day_rate = workload_metrics.get("work_day_external_pr_rate", 0)
        business_days = workload_metrics.get("total_analysis_business_days", 0)

        f.write("| Metric | Value | Status |\n")
        f.write("|--------|--------|--------|\n")
        f.write(f"| Total External PRs | {total_prs} | - |\n")
        f.write(f"| Analysis Period (Work Days) | {business_days} days | - |\n")
        f.write(
            f"| External PRs per Work Day | {work_day_rate:.2f} PRs/day | {self._get_work_day_rate_status(work_day_rate)} |\n"
        )
        f.write(f"| Average Lead Time | {avg_lead_time:.1f} days | {self._get_lead_time_status(avg_lead_time)} |\n")
        f.write(f"| Average Reviews per PR | {avg_reviews:.1f} | {self._get_reviews_status(avg_reviews)} |\n")
        f.write(f"| Average PR Size | {avg_size:.0f} lines | {self._get_size_status(avg_size)} |\n")
        f.write(f"| **Overall Pressure Score** | **{score:.3f}** | **{level}** |\n")

        # Breakdown of pressure factors
        f.write("\n**Pressure Factor Breakdown:**\n")

        # Calculate individual factor contributions
        external_factor = min(100.0 / 50, 1.0) * 0.3  # All PRs are external
        lead_time_factor = min(avg_lead_time / 10, 1.0) * 0.25
        reviews_factor = min(avg_reviews / 5, 1.0) * 0.25
        size_factor = min(avg_size / 1000, 1.0) * 0.2

        f.write(f"- External Ratio Impact: {external_factor:.3f} (weight: 30%)\n")
        f.write(f"- Lead Time Impact: {lead_time_factor:.3f} (weight: 25%)\n")
        f.write(f"- Review Burden Impact: {reviews_factor:.3f} (weight: 25%)\n")
        f.write(f"- PR Size Impact: {size_factor:.3f} (weight: 20%)\n")
        f.write(f"- **Total Score**: {score:.3f} → **{level} PRESSURE**\n\n")

    def _interpret_correlation(self, correlation: float) -> str:
        """Interpret correlation coefficient."""
        abs_corr = abs(correlation)
        direction = "Positive" if correlation > 0 else "Negative"

        if abs_corr > 0.7:
            strength = "Strong"
        elif abs_corr > 0.3:
            strength = "Moderate"
        elif abs_corr > 0.1:
            strength = "Weak"
        else:
            strength = "No"
            direction = ""

        return f"{strength} {direction}".strip()

    def _get_metric_status(self, score: float) -> str:
        """Get status indicator for metric score."""
        if score > 0.7:
            return "🔴 HIGH"
        elif score > 0.4:
            return "🟡 MEDIUM"
        else:
            return "🟢 LOW"

    def _get_lead_time_status(self, lead_time_days: float) -> str:
        """Get status indicator for lead time."""
        if lead_time_days > 5:
            return "🔴 HIGH"
        elif lead_time_days > 2:
            return "🟡 MEDIUM"
        else:
            return "🟢 LOW"

    def _get_reviews_status(self, avg_reviews: float) -> str:
        """Get status indicator for review burden."""
        if avg_reviews > 4:
            return "🔴 HIGH"
        elif avg_reviews > 2:
            return "🟡 MEDIUM"
        else:
            return "🟢 LOW"

    def _get_size_status(self, avg_size: float) -> str:
        """Get status indicator for PR size."""
        if avg_size > 800:
            return "🔴 LARGE"
        elif avg_size > 300:
            return "🟡 MEDIUM"
        else:
            return "🟢 SMALL"

    def _get_work_day_rate_status(self, work_day_rate: float) -> str:
        """Get status indicator for work-day PR rate."""
        if work_day_rate > 2.0:  # More than 2 PRs per work day
            return "🔴 HIGH"
        elif work_day_rate > 1.0:  # More than 1 PR per work day
            return "🟡 MEDIUM"
        else:
            return "🟢 LOW"

    def _write_metrics_explanation_section(self, f, pressure_metrics: dict[str, Any], correlations: dict[str, Any]):
        """Write detailed explanation of metrics and their significance."""
        f.write("## 📋 Understanding the Metrics\n\n")

        external_fraction = pressure_metrics.get("external_pr_fraction", 0)
        team_size = pressure_metrics.get("team_size", 6)

        f.write("### 🎯 Key Metrics Breakdown\n\n")

        f.write("**External PR Fraction**: ")
        if external_fraction > 0.6:
            f.write(f"{external_fraction:.1%} of all PRs come from external contributors. ")
            f.write(
                "🔴 **HIGH** - This represents significant external contribution volume that may strain review capacity.\n\n"
            )
        elif external_fraction > 0.4:
            f.write(f"{external_fraction:.1%} of all PRs come from external contributors. ")
            f.write(
                "🟡 **MODERATE** - External contributions are substantial and should be monitored for team impact.\n\n"
            )
        else:
            f.write(f"{external_fraction:.1%} of all PRs come from external contributors. ")
            f.write("🟢 **LOW** - External contribution volume is manageable for the current team size.\n\n")

        f.write("**Team Capacity Analysis**:\n")
        f.write(f"- With **{team_size} engineers** available for reviews\n")
        f.write(
            f"- Each engineer handles approximately **{pressure_metrics.get('prs_per_engineer', 0):.1f} total PRs**\n"
        )
        f.write(
            f"- Each engineer reviews **{pressure_metrics.get('external_prs_per_engineer', 0):.1f} external PRs**\n\n"
        )

        if pressure_metrics.get("external_prs_per_engineer", 0) > 8:
            f.write(
                "⚠️ **Capacity Warning**: High external PR load per engineer may impact review quality and response times.\n\n"
            )
        elif pressure_metrics.get("external_prs_per_engineer", 0) > 5:
            f.write("📊 **Moderate Load**: External PR load is noticeable but generally manageable.\n\n")
        else:
            f.write("✅ **Healthy Load**: External PR distribution is well within team capacity.\n\n")

        f.write("### 📊 Pressure Score Components\n\n")

        pressure_formula = pressure_metrics.get("pressure_formula", {})
        weights = pressure_formula.get("weights", {})

        f.write("The **Pressure Score** is calculated using weighted factors:\n\n")
        f.write(
            f"- **External Ratio** ({weights.get('external_ratio_weight', 0.3):.0%}): How much of total PR volume is external\n"
        )
        f.write(
            f"- **Lead Time** ({weights.get('lead_time_weight', 0.25):.0%}): Average time from creation to merge/close\n"
        )
        f.write(f"- **Review Burden** ({weights.get('reviews_burden_weight', 0.25):.0%}): Average reviews per PR\n")
        f.write(f"- **PR Complexity** ({weights.get('pr_size_weight', 0.15):.0%}): Average PR size (lines changed)\n")
        f.write(f"- **Rework Factor** ({weights.get('review_rounds_weight', 0.05):.0%}): Review rounds needed\n\n")

        score = pressure_metrics.get("pressure_score", 0)
        if score >= 0.7:
            f.write(
                f"**Score: {score:.3f}/1.000** - 🔴 **CRITICAL**: Immediate action needed to prevent team burnout\n\n"
            )
        elif score >= 0.4:
            f.write(
                f"**Score: {score:.3f}/1.000** - 🟡 **ELEVATED**: Monitor closely and consider preventive measures\n\n"
            )
        else:
            f.write(f"**Score: {score:.3f}/1.000** - 🟢 **MANAGEABLE**: Current workload is sustainable\n\n")

    def _write_correlation_explanation_section(self, f, correlations: dict[str, Any]):
        """Write explanation of correlation analysis and what it indicates."""
        f.write("## 🔗 Correlation Analysis Insights\n\n")

        f.write("### 📈 Understanding Correlations\n\n")
        f.write("Correlations measure how strongly two metrics move together. Values range from -1.0 to +1.0:\n\n")
        f.write("- **+0.7 to +1.0**: Strong positive relationship (as one increases, the other increases)\n")
        f.write("- **+0.3 to +0.7**: Moderate positive relationship\n")
        f.write("- **-0.3 to +0.3**: Weak or no relationship\n")
        f.write("- **-0.7 to -0.3**: Moderate negative relationship\n")
        f.write("- **-1.0 to -0.7**: Strong negative relationship (as one increases, the other decreases)\n\n")

        # Get key correlations
        lead_time_correlations = correlations.get("lead_time_correlations", {})
        strong_correlations = correlations.get("strong_correlations", {})

        f.write("### 🎯 Key Correlation Findings\n\n")

        # PR Size vs Lead Time
        size_lead_correlation = strong_correlations.get("pr_size", 0)
        f.write(f"**PR Size vs Lead Time**: {size_lead_correlation:.3f}\n")
        if abs(size_lead_correlation) > 0.5:
            if size_lead_correlation > 0:
                f.write("- 🔴 **Strong Positive**: Larger PRs significantly increase merge time\n")
                f.write("- **Impact**: This suggests PR size is a bottleneck - consider breaking down large PRs\n")
                f.write("- **Action**: Implement PR size guidelines and automated warnings\n\n")
            else:
                f.write("- 🟢 **Strong Negative**: Larger PRs are processed faster (unusual but possible)\n")
                f.write("- **Possible Reason**: Large PRs may get priority attention or batch processing\n\n")
        elif abs(size_lead_correlation) > 0.3:
            f.write("- 🟡 **Moderate**: PR size has some influence on lead time\n")
            f.write("- **Monitoring**: Watch for trends, consider size guidelines\n\n")
        else:
            f.write("- 🟢 **Weak**: PR size has minimal impact on lead time\n")
            f.write("- **Good Sign**: Review process is consistent regardless of PR size\n\n")

        # Reviews vs Lead Time
        reviews_lead_correlation = lead_time_correlations.get("reviews_count", 0)
        f.write(f"**Review Count vs Lead Time**: {reviews_lead_correlation:.3f}\n")
        if reviews_lead_correlation > 0.5:
            f.write("- 🔴 **High Impact**: More reviews significantly delay merge time\n")
            f.write("- **Potential Issue**: Review process may be inefficient or PRs are complex\n")
            f.write("- **Consider**: Streamlining review process, clearer acceptance criteria\n\n")
        elif reviews_lead_correlation > 0.3:
            f.write("- 🟡 **Moderate Impact**: Additional reviews somewhat increase lead time\n")
            f.write("- **Normal**: Expected relationship, but monitor for efficiency\n\n")
        else:
            f.write("- 🟢 **Low Impact**: Review count has minimal effect on lead time\n")
            f.write("- **Efficient Process**: Reviews are conducted effectively\n\n")

        # Changed Files vs Lead Time
        files_lead_correlation = lead_time_correlations.get("changed_files", 0)
        f.write(f"**Changed Files vs Lead Time**: {files_lead_correlation:.3f}\n")
        if files_lead_correlation > 0.5:
            f.write("- 🔴 **High Complexity**: PRs touching many files take much longer\n")
            f.write("- **Risk**: Cross-cutting changes may indicate architectural issues\n")
            f.write("- **Action**: Consider refactoring to reduce file coupling\n\n")
        elif files_lead_correlation > 0.3:
            f.write("- 🟡 **Some Complexity**: Multi-file changes increase review time moderately\n")
            f.write("- **Expected**: Normal relationship, but monitor scope creep\n\n")
        else:
            f.write("- 🟢 **Well Scoped**: File count doesn't significantly impact review time\n")
            f.write("- **Good Practice**: PRs are well-focused and scoped appropriately\n\n")

        f.write("### 🎯 What These Correlations Mean for Your Team\n\n")

        # Provide actionable insights based on correlation patterns
        high_correlations = [k for k, v in strong_correlations.items() if abs(v) > 0.5]

        if len(high_correlations) > 2:
            f.write("**⚠️ Multiple Strong Correlations Detected**\n")
            f.write(
                "Your team is experiencing predictable bottlenecks. The review process has clear patterns that can be optimized:\n\n"
            )
            f.write("**Immediate Actions**:\n")
            f.write("1. Set up automated PR size warnings\n")
            f.write("2. Create review time SLA based on PR characteristics\n")
            f.write("3. Consider pairing/mob review for complex PRs\n")
            f.write("4. Implement pre-review checklists to reduce iterations\n\n")
        elif len(high_correlations) > 0:
            f.write("**📊 Some Process Patterns Identified**\n")
            f.write("Your review process shows some predictable relationships that can be optimized.\n\n")
        else:
            f.write("**✅ Efficient Review Process**\n")
            f.write(
                "Your team maintains consistent review times regardless of PR characteristics. This indicates a mature, efficient process.\n\n"
            )

    def _get_analysis_period(self, df: pd.DataFrame) -> str:
        """Get analysis period from dataframe."""
        if "created_at" in df.columns:
            start_date = pd.to_datetime(df["created_at"]).min()
            end_date = pd.to_datetime(df["created_at"]).max()
            return f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
        return "Unknown period"

    def _generate_html_dashboard(
        self,
        df: pd.DataFrame,
        monthly_trends: dict[str, Any],
        correlations: dict[str, Any],
        pressure_metrics: dict[str, Any],
        insights: dict[str, list[str]],
        output_dir: str,
    ) -> str:
        """Generate HTML dashboard with interactive visualization."""
        self.logger.info("Generating HTML dashboard")

        dashboard_file = os.path.join(output_dir, "pr_workload_dashboard.html")

        # Get analysis metadata
        total_prs = len(df)
        external_prs = len(df[~df["is_team_member"]])
        external_percentage = (external_prs / total_prs * 100) if total_prs > 0 else 0
        pressure_level = pressure_metrics.get("pressure_level", "UNKNOWN")
        pressure_score = pressure_metrics.get("pressure_score", 0)
        analysis_period = self._get_analysis_period(df)

        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>PR Workload Analysis Dashboard</title>
            <style>
                body {{
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 10px;
                    margin-bottom: 20px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 2.5em;
                }}
                .header .subtitle {{
                    margin: 10px 0 0 0;
                    opacity: 0.9;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}
                .metrics-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .metric-card {{
                    background: white;
                    padding: 25px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    text-align: center;
                }}
                .metric-value {{
                    font-size: 2.5em;
                    font-weight: bold;
                    margin-bottom: 10px;
                }}
                .metric-label {{
                    color: #666;
                    font-size: 0.9em;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                }}
                .pressure-high {{ color: #e74c3c; }}
                .pressure-medium {{ color: #f39c12; }}
                .pressure-low {{ color: #27ae60; }}
                .alert {{
                    padding: 20px;
                    border-radius: 10px;
                    margin-bottom: 20px;
                    font-weight: bold;
                }}
                .alert-high {{
                    background-color: #f8d7da;
                    border: 1px solid #f5c6cb;
                    color: #721c24;
                }}
                .alert-medium {{
                    background-color: #fff3cd;
                    border: 1px solid #ffeaa7;
                    color: #856404;
                }}
                .alert-low {{
                    background-color: #d4edda;
                    border: 1px solid #c3e6cb;
                    color: #155724;
                }}
                .section {{
                    background: white;
                    padding: 30px;
                    border-radius: 10px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                    margin-bottom: 20px;
                }}
                .section h2 {{
                    margin-top: 0;
                    color: #333;
                    border-bottom: 3px solid #667eea;
                    padding-bottom: 10px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-top: 15px;
                }}
                th, td {{
                    padding: 12px;
                    text-align: left;
                    border-bottom: 1px solid #ddd;
                }}
                th {{
                    background-color: #f8f9fa;
                    font-weight: bold;
                    color: #333;
                }}
                tr:hover {{
                    background-color: #f5f5f5;
                }}
                .chart-placeholder {{
                    background: #f8f9fa;
                    padding: 40px;
                    text-align: center;
                    border-radius: 8px;
                    margin: 20px 0;
                    color: #666;
                }}
                .insights-list {{
                    list-style: none;
                    padding: 0;
                }}
                .insights-list li {{
                    background: #f8f9fa;
                    padding: 15px;
                    margin-bottom: 10px;
                    border-radius: 8px;
                    border-left: 4px solid #667eea;
                }}
                .recommendations-list {{
                    list-style: none;
                    padding: 0;
                }}
                .recommendations-list li {{
                    background: #fff3cd;
                    padding: 15px;
                    margin-bottom: 10px;
                    border-radius: 8px;
                    border-left: 4px solid #f39c12;
                }}
                .footer {{
                    text-align: center;
                    margin-top: 40px;
                    padding: 20px;
                    color: #666;
                    font-size: 0.9em;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔍 PR Workload Analysis Dashboard</h1>
                    <p class="subtitle">Generated on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")} | Period: {analysis_period}</p>
                </div>
        """

        # Pressure level alert
        if pressure_level == "HIGH":
            html_content += f"""
                <div class="alert alert-high">
                    🚨 <strong>CRITICAL ALERT:</strong> CODEOWNERS are under HIGH pressure (Score: {pressure_score:.3f})
                    <br>Immediate action required to prevent bottlenecks and team burnout.
                </div>
            """
        elif pressure_level == "MEDIUM":
            html_content += f"""
                <div class="alert alert-medium">
                    ⚠️ <strong>WARNING:</strong> CODEOWNERS are under MEDIUM pressure (Score: {pressure_score:.3f})
                    <br>Monitor closely and consider preventive measures.
                </div>
            """
        else:
            html_content += f"""
                <div class="alert alert-low">
                    ✅ <strong>STATUS OK:</strong> CODEOWNERS pressure is LOW (Score: {pressure_score:.3f})
                    <br>Current workload appears manageable.
                </div>
            """

        # Metrics cards
        pressure_class = f"pressure-{pressure_level.lower()}"
        html_content += f"""
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value">{total_prs}</div>
                        <div class="metric-label">Total PRs Analyzed</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{external_prs}</div>
                        <div class="metric-label">External Author PRs</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value">{external_percentage:.1f}%</div>
                        <div class="metric-label">External Contribution Rate</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value {pressure_class}">{pressure_level}</div>
                        <div class="metric-label">Pressure Level</div>
                    </div>
                </div>
        """

        # Lead Time Statistics Section
        mean_lead_time = df["lead_time_days"].mean()
        median_lead_time = df["lead_time_days"].median()
        p95_lead_time = df["lead_time_days"].quantile(0.95)

        external_df = df[~df["is_team_member"]]
        team_df = df[df["is_team_member"]]
        ext_mean = external_df["lead_time_days"].mean() if len(external_df) > 0 else 0
        team_mean = team_df["lead_time_days"].mean() if len(team_df) > 0 else 0

        html_content += f"""
                <div class="section">
                    <h2>⏱️ Lead Time Analysis</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Metric</th>
                                <th>External PRs</th>
                                <th>Team PRs</th>
                                <th>Overall</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><strong>Mean Lead Time</strong></td>
                                <td>{ext_mean:.1f} days</td>
                                <td>{team_mean:.1f} days</td>
                                <td>{mean_lead_time:.1f} days</td>
                            </tr>
                            <tr>
                                <td><strong>Median Lead Time</strong></td>
                                <td>{external_df["lead_time_days"].median():.1f} days</td>
                                <td>{team_df["lead_time_days"].median():.1f} days</td>
                                <td>{median_lead_time:.1f} days</td>
                            </tr>
                            <tr>
                                <td><strong>95th Percentile</strong></td>
                                <td>{external_df["lead_time_days"].quantile(0.95):.1f} days</td>
                                <td>{team_df["lead_time_days"].quantile(0.95):.1f} days</td>
                                <td>{p95_lead_time:.1f} days</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
        """

        # Correlation Analysis Section
        size_lead_corr = correlations.get("size_lead_time_correlation", 0)
        external_lead_corr = correlations.get("external_lead_time_correlation", 0)

        html_content += f"""
                <div class="section">
                    <h2>🔗 Correlation Analysis</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Variables</th>
                                <th>Correlation</th>
                                <th>Interpretation</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td>PR Size vs Lead Time</td>
                                <td>{size_lead_corr:.3f}</td>
                                <td>{self._interpret_correlation(size_lead_corr)}</td>
                            </tr>
                            <tr>
                                <td>External Author vs Lead Time</td>
                                <td>{external_lead_corr:.3f}</td>
                                <td>{self._interpret_correlation(external_lead_corr)}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
        """

        # Key Insights Section
        html_content += """
                <div class="section">
                    <h2>🔍 Key Insights</h2>
                    <ul class="insights-list">
        """

        for insight in insights["key_points"]:
            html_content += f"<li>{insight}</li>"

        html_content += """
                    </ul>
                </div>
        """

        # Recommendations Section
        html_content += """
                <div class="section">
                    <h2>💡 Recommendations</h2>
                    <ul class="recommendations-list">
        """

        for recommendation in insights["recommendations"]:
            html_content += f"<li>{recommendation}</li>"

        html_content += """
                    </ul>
                </div>
        """

        # Charts placeholder section
        html_content += """
                <div class="section">
                    <h2>📊 Visualizations</h2>
                    <p>Charts have been generated as separate PNG files in the charts/ directory:</p>
                    <div class="chart-placeholder">
                        📈 Lead Time Distribution Chart<br>
                        <small>File: charts/lead_time_distribution.png</small>
                    </div>
                    <div class="chart-placeholder">
                        🔥 Correlation Heatmap<br>
                        <small>File: charts/correlation_heatmap.png</small>
                    </div>
                    <div class="chart-placeholder">
                        📏 PR Size vs Lead Time Scatter Plot<br>
                        <small>File: charts/pr_size_vs_lead_time.png</small>
                    </div>
                </div>
        """

        # Footer
        html_content += f"""
                <div class="footer">
                    <p>Report generated by <strong>PyToolkit PR Workload Analysis</strong><br>
                    Analysis completed on {datetime.now().strftime("%Y-%m-%d at %H:%M:%S")}</p>
                </div>
            </div>
        </body>
        </html>
        """

        with open(dashboard_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        self.logger.info(f"HTML dashboard saved: {dashboard_file}")
        return dashboard_file
