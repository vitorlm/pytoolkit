"""
LinearB Knowledge Sharing Metrics Service.

This service analyzes PR review patterns and knowledge distribution metrics
using LinearB API data.
"""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager

from domains.syngenta.jira.issue_adherence_service import TimePeriodParser

from .linearb_api_client import (
    LinearBApiClient,
    LinearBGroupBy,
    LinearBMetrics,
    LinearBRollup,
)


class KnowledgeSharingMetricsService:
    """Service for analyzing knowledge sharing patterns and metrics."""

    def __init__(self):
        """Initialize the Knowledge Sharing Metrics service."""
        self.logger = LogManager.get_instance().get_logger("KnowledgeSharingMetricsService")
        self.linearb_client = LinearBApiClient()
        self.cache = CacheManager.get_instance()

    def get_knowledge_sharing_metrics(
        self,
        team_ids: List[str],
        time_range: str,
        pr_threshold: int = 5,
        verbose: bool = False,
    ) -> Dict[str, Any]:
        """
        Get knowledge sharing metrics for teams.

        Args:
            team_ids: List of LinearB team IDs
            time_range: Time period (last-week, last-month, last-2-weeks, custom dates)
            pr_threshold: Minimum PRs for inclusion (default: 5)
            verbose: Enable detailed output

        Returns:
            Knowledge sharing metrics data
        """
        try:
            # Parse time range
            time_parser = TimePeriodParser()
            try:
                start_date, end_date = time_parser.parse_time_period(time_range)
            except ValueError as e:
                raise ValueError(f"Invalid time range format: {time_range}. Supported formats: 'last-week', 'last-2-weeks', 'last-month', or 'YYYY-MM-DD to YYYY-MM-DD' for custom ranges. Error: {str(e)}")

            # Convert team IDs to integers
            try:
                team_ids_int = [int(tid) for tid in team_ids]
            except ValueError as e:
                raise ValueError(f"Invalid team IDs format. Please provide comma-separated integers. Error: {str(e)}")

            # Create cache key
            cache_key = f"knowledge_metrics_{','.join(team_ids)}_{time_range}_{pr_threshold}"
            cached_data = self.cache.load(cache_key, expiration_minutes=60)

            if cached_data is not None:
                self.logger.info("Using cached knowledge metrics data")
                return cached_data

            # Fetch PR metrics from LinearB API
            metrics_data = self._fetch_pr_metrics(team_ids_int, start_date, end_date, pr_threshold)

            # Process metrics to calculate knowledge sharing indicators
            results = self._calculate_knowledge_sharing_metrics(metrics_data, team_ids, time_range, start_date, end_date)

            # Cache the results
            self.cache.save(cache_key, results)

            self.logger.info("Successfully calculated knowledge sharing metrics")
            return results

        except Exception as e:
            self.logger.error(f"Failed to get knowledge sharing metrics: {e}", exc_info=True)
            raise

    def _fetch_pr_metrics(
        self,
        team_ids: List[int],
        start_date: datetime,
        end_date: datetime,
        pr_threshold: int,
    ) -> Dict[str, Any]:
        """
        Fetch PR metrics from LinearB API.

        Args:
            team_ids: List of LinearB team IDs
            start_date: Start date for metrics
            end_date: End date for metrics
            pr_threshold: Minimum PRs for inclusion

        Returns:
            Raw metrics data from LinearB API
        """
        try:
            # Format dates for LinearB API
            time_ranges = [
                {
                    "after": start_date.strftime("%Y-%m-%d"),
                    "before": end_date.strftime("%Y-%m-%d"),
                }
            ]

            # Define the metrics we want to fetch
            requested_metrics = [
                {"name": LinearBMetrics.PR_REVIEWED, "agg": "default"},  # PRs reviewed
                {"name": LinearBMetrics.PR_REVIEWS, "agg": "default"},  # Reviews given
                {"name": LinearBMetrics.REVIEW_TIME, "agg": "avg"},  # Average review time
                {"name": LinearBMetrics.PR_MERGED, "agg": "default"},  # PRs merged
                {"name": LinearBMetrics.PR_NEW, "agg": "default"},  # New PRs
            ]

            self.logger.info(f"Fetching PR metrics for teams: {team_ids}")
            self.logger.info(f"Time range: {start_date} to {end_date}")

            # Get metrics from LinearB API
            metrics_data = self.linearb_client.get_metrics(
                requested_metrics=requested_metrics,
                time_ranges=time_ranges,
                group_by=LinearBGroupBy.CONTRIBUTOR,
                team_ids=team_ids,
                roll_up=LinearBRollup.CUSTOM,
            )

            self.logger.info(f"Retrieved metrics for {len(metrics_data)} contributors")
            return metrics_data

        except Exception as e:
            self.logger.error(f"Failed to fetch PR metrics: {e}", exc_info=True)
            raise

    def _debug_api_response(self, metrics_data: Dict[str, Any]):
        """
        Debug the API response to understand the data structure.
        
        Args:
            metrics_data: Raw metrics data from LinearB API
        """
        try:
            self.logger.debug("=== API RESPONSE DEBUG INFO ===")
            self.logger.debug(f"Response type: {type(metrics_data)}")
            
            if isinstance(metrics_data, list):
                self.logger.debug(f"Response is list with {len(metrics_data)} items")
                for i, item in enumerate(metrics_data[:2]):  # Only show first 2 items
                    self.logger.debug(f"Item {i} keys: {list(item.keys()) if isinstance(item, dict) else 'Not a dict'}")
                    if isinstance(item, dict) and 'metrics' in item:
                        self.logger.debug(f"Item {i} metrics count: {len(item['metrics']) if isinstance(item['metrics'], list) else 'Not a list'}")
                        if isinstance(item['metrics'], list) and len(item['metrics']) > 0:
                            sample_contributor = item['metrics'][0]
                            self.logger.debug(f"Sample contributor keys: {list(sample_contributor.keys())}")
                            # Log a few key fields
                            for key in ['name', 'pr.reviews', 'pr.reviewed', 'pr.merged', 'branch.review_time']:
                                if key in sample_contributor:
                                    self.logger.debug(f"  {key}: {sample_contributor[key]}")
            elif isinstance(metrics_data, dict):
                self.logger.debug(f"Response is dict with keys: {list(metrics_data.keys())}")
                if 'metrics' in metrics_data:
                    self.logger.debug(f"Metrics type: {type(metrics_data['metrics'])}")
                    if isinstance(metrics_data['metrics'], list):
                        self.logger.debug(f"Metrics count: {len(metrics_data['metrics'])}")
                        if len(metrics_data['metrics']) > 0:
                            sample_contributor = metrics_data['metrics'][0]
                            self.logger.debug(f"Sample contributor keys: {list(sample_contributor.keys())}")
                            # Log a few key fields
                            for key in ['name', 'pr.reviews', 'pr.reviewed', 'pr.merged', 'branch.review_time']:
                                if key in sample_contributor:
                                    self.logger.debug(f"  {key}: {sample_contributor[key]}")
            
            self.logger.debug("=== END API RESPONSE DEBUG INFO ===")
        except Exception as e:
            self.logger.error(f"Failed to debug API response: {e}")

    def _calculate_knowledge_sharing_metrics(
        self,
        metrics_data: Dict[str, Any],
        team_ids: List[str],
        time_range: str,
        start_date: datetime,
        end_date: datetime,
    ) -> Dict[str, Any]:
        """
        Calculate knowledge sharing metrics from raw data.

        Args:
            metrics_data: Raw metrics data from LinearB API
            team_ids: List of LinearB team IDs
            time_range: Time period
            start_date: Start date
            end_date: End date

        Returns:
            Processed knowledge sharing metrics
        """
        try:
            # Initialize results structure
            results = {
                "metadata": {
                    "team_ids": team_ids,
                    "time_range": time_range,
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "analysis_date": datetime.now().isoformat(),
                },
                "metrics": {},
                "team_analysis": {
                    "reviewers": [],
                    "repositories": [],
                },
                "insights": [],
            }

            # Extract contributor metrics
            contributors_data = []
            if isinstance(metrics_data, list) and len(metrics_data) > 0:
                # Handle list format (time periods)
                for period in metrics_data:
                    if "metrics" in period:
                        contributors_data.extend(period["metrics"])
            elif isinstance(metrics_data, dict) and "metrics" in metrics_data:
                # Handle dict format
                contributors_data = metrics_data["metrics"]
            
            # Debug the API response to understand the data structure
            self._debug_api_response(metrics_data)

            # Process contributor data
            reviewers = []
            total_prs_reviewed = 0
            total_review_time = 0
            review_time_count = 0
            
            # Track data quality issues
            unknown_names_count = 0
            null_review_times = 0

            for contributor in contributors_data:
                # Extract metrics for each contributor
                name = contributor.get("name", "Unknown")
                if name == "Unknown" or not name or name.lower() == "unknown":
                    unknown_names_count += 1
                    name = f"Reviewer-{len(reviewers)+1}"  # Give them a placeholder name
                    
                reviews_given = contributor.get("pr.reviews", 0)
                reviews_received = contributor.get("pr.reviewed", 0)
                prs_merged = contributor.get("pr.merged", 0)
                
                # Calculate average response time if available
                avg_response_time = None
                if "branch.review_time" in contributor and contributor["branch.review_time"] is not None:
                    avg_response_time_hours = contributor["branch.review_time"] / 3600  # Convert seconds to hours
                    avg_response_time = round(avg_response_time_hours, 1)
                    total_review_time += avg_response_time_hours
                    review_time_count += 1
                else:
                    null_review_times += 1

                # Only include contributors with significant activity
                if reviews_given + reviews_received + prs_merged > 0:
                    reviewer = {
                        "name": name,
                        "reviews_given": reviews_given,
                        "reviews_received": reviews_received,
                        "prs_merged": prs_merged,
                        "avg_response_time_hours": avg_response_time,
                        "expertise_areas": [],  # Placeholder for future implementation
                    }
                    reviewers.append(reviewer)
                    total_prs_reviewed += reviews_given

            # Add data quality warnings to insights if needed
            if unknown_names_count > 0:
                results["insights"].append(f"Notice: {unknown_names_count} contributors have anonymized names due to privacy settings")
            
            if null_review_times == len(contributors_data) and len(contributors_data) > 0:
                results["insights"].append("Notice: Review time data unavailable - may require different API permissions or metrics")

            # Calculate overall metrics
            unique_reviewers = len(reviewers)
            average_review_time_hours = round(total_review_time / review_time_count, 1) if review_time_count > 0 else 0

            # Simple bus factor calculation (top 20% of reviewers by reviews given)
            reviewers_sorted = sorted(reviewers, key=lambda x: x["reviews_given"], reverse=True)
            top_reviewers_count = max(1, int(len(reviewers_sorted) * 0.2))
            bus_factor = min(top_reviewers_count, len(reviewers_sorted))

            # Simple knowledge distribution score (based on review distribution)
            if reviewers:
                total_reviews = sum(r["reviews_given"] for r in reviewers)
                if total_reviews > 0:
                    # Calculate distribution evenness (0-1 scale, 1 is perfectly even)
                    avg_reviews_per_reviewer = total_reviews / len(reviewers)
                    variance = sum(abs(r["reviews_given"] - avg_reviews_per_reviewer) for r in reviewers) / len(reviewers)
                    normalized_variance = variance / avg_reviews_per_reviewer if avg_reviews_per_reviewer > 0 else 0
                    knowledge_distribution_score = max(0, 1 - normalized_variance)
                else:
                    knowledge_distribution_score = 0
            else:
                knowledge_distribution_score = 0

            # Add metrics to results
            results["metrics"] = {
                "total_prs_reviewed": total_prs_reviewed,
                "unique_reviewers": unique_reviewers,
                "average_review_time_hours": average_review_time_hours,
                "bus_factor": bus_factor,
                "knowledge_distribution_score": round(knowledge_distribution_score, 2),
            }

            # Add reviewers to team analysis
            results["team_analysis"]["reviewers"] = reviewers

            # Generate insights
            insights = []
            if bus_factor <= 2:
                insights.append("Bus factor of 2 or less indicates potential knowledge risk")
            elif bus_factor <= 3:
                insights.append("Moderate bus factor - some knowledge concentration risk")
            else:
                insights.append("Healthy bus factor - good knowledge distribution")

            # Only provide review time insights if we have data
            if review_time_count > 0:
                if average_review_time_hours > 8:
                    insights.append("Review response time is higher than recommended (> 8 hours)")
                elif average_review_time_hours > 0:
                    insights.append("Review response time within healthy range (< 8 hours)")
            else:
                insights.append("Review time data not available from API - may require different permissions")

            if knowledge_distribution_score < 0.5:
                insights.append("Low knowledge distribution score indicates high concentration")
            elif knowledge_distribution_score > 0.8:
                insights.append("High knowledge distribution score indicates good sharing")
            else:
                insights.append("Moderate knowledge distribution - balanced sharing")

            # Add data quality insights
            if unknown_names_count > 0:
                insights.append(f"{unknown_names_count} contributors have anonymized names due to privacy settings")
            
            if null_review_times == len(contributors_data) and len(contributors_data) > 0:
                insights.append("Review time data unavailable - may require different API permissions or metrics")

            results["insights"] = insights

            return results

        except Exception as e:
            self.logger.error(f"Failed to calculate knowledge sharing metrics: {e}", exc_info=True)
            raise

    def save_results(self, results: Dict[str, Any], output_file: Optional[str] = None) -> str:
        """
        Save results to a JSON file.

        Args:
            results: Results data to save
            output_file: Optional output file path

        Returns:
            Path to the saved file
        """
        try:
            if output_file:
                output_path = output_file
            else:
                output_path = OutputManager.get_output_path(
                    "knowledge-sharing", f"knowledge_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                )

            # Ensure output directory exists
            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            JSONManager.write_json(results, output_path)
            self.logger.info(f"Results saved to: {output_path}")
            return output_path

        except Exception as e:
            self.logger.error(f"Failed to save results: {e}", exc_info=True)
            raise