"""
GitHub Pull Request External Contributors Analysis Service

Business logic for analyzing PRs from external contributors and computing metrics.
"""

import csv
import os
import statistics
from argparse import Namespace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.error.error_manager import handle_generic_exception
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager

from .github_api_client import GitHubApiClient


class PrAnalysisService:
    """Service for analyzing external contributor PRs and computing lead time metrics."""

    def __init__(self, max_workers: int = 4):
        self.logger = LogManager.get_instance().get_logger("PrAnalysisService")
        self.cache = CacheManager.get_instance()
        self.github_client = GitHubApiClient(max_workers=max_workers)

    def analyze_external_prs(self, args: Namespace) -> Dict[str, Any]:
        """
        Main method to analyze external contributor PRs.

        Args:
            args: Command line arguments from ArgumentParser

        Returns:
            Dictionary with analysis results and metadata
        """
        try:
            self.logger.info("Starting external PR analysis")

            # Step 1: Resolve team members
            team_members = self._resolve_team_members(args.codeowners_teams)

            # Step 2: Get target repositories
            repos = self._get_target_repositories(args)

            # Step 3: Collect and analyze PRs
            pr_data = self._collect_pr_data(repos, team_members, args)

            # Step 4: Calculate metrics
            metrics = self._calculate_metrics(pr_data, args)

            # Step 5: Generate outputs
            self._generate_outputs(pr_data, metrics, args)

            # Step 6: Return summary
            results = {
                "total_prs": len(pr_data),
                "external_prs": len([pr for pr in pr_data if not pr["is_team_member"]]),
                "team_members_count": len(team_members),
                "repositories_analyzed": len(repos),
                "metrics": metrics,
                "output_files": self._get_output_files(args),
            }

            self.logger.info("External PR analysis completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
            handle_generic_exception(e, "PR external analysis", {"args": str(args)})
            raise

    def _resolve_team_members(self, team_specs: List[str]) -> set:
        """Resolve team members from CODEOWNERS team specifications."""
        self.logger.info(f"Resolving team members from {len(team_specs)} teams")

        try:
            team_members = self.github_client.resolve_team_members_parallel(team_specs)
            self.logger.info(f"Resolved {len(team_members)} unique team members")
            return team_members
        except Exception as e:
            self.logger.error(f"Failed to resolve team members: {e}")
            raise

    def _get_target_repositories(self, args: Namespace) -> List[Dict[str, Any]]:
        """Get the list of repositories to analyze."""
        self.logger.info("Getting target repositories")

        try:
            if args.repos:
                # Specific repositories
                repos = []
                for repo_name in args.repos:
                    repo_info = {
                        "name": repo_name,
                        "full_name": f"{args.org}/{repo_name}",
                        "owner": {"login": args.org},
                    }
                    repos.append(repo_info)
                self.logger.info(f"Targeting {len(repos)} specific repositories")
            else:
                # All repositories in org
                repos = self.github_client.get_org_repos(
                    args.org, args.include_archived
                )
                self.logger.info(f"Found {len(repos)} repositories in organization")

            return repos
        except Exception as e:
            self.logger.error(f"Failed to get target repositories: {e}")
            raise

    def _collect_pr_data(
        self, repos: List[Dict[str, Any]], team_members: set, args: Namespace
    ) -> List[Dict[str, Any]]:
        """Collect PR data from all target repositories."""
        self.logger.info(f"Collecting PR data from {len(repos)} repositories")

        all_pr_data = []

        for repo in repos:
            try:
                repo_name = repo["name"]
                owner = repo["owner"]["login"] if "owner" in repo else args.org

                self.logger.info(f"Processing repository: {owner}/{repo_name}")

                # Get PRs for this repository
                prs = self.github_client.get_pull_requests(owner, repo_name, args.state)

                for pr in prs:
                    pr_data = self._process_single_pr(
                        pr, owner, repo_name, team_members, args
                    )
                    if pr_data:
                        all_pr_data.append(pr_data)

            except Exception as e:
                self.logger.error(
                    f"Failed to process repository {repo.get('name', 'unknown')}: {e}"
                )
                continue

        self.logger.info(f"Collected data for {len(all_pr_data)} PRs")
        return all_pr_data

    def _process_single_pr(
        self,
        pr: Dict[str, Any],
        owner: str,
        repo_name: str,
        team_members: set,
        args: Namespace,
    ) -> Optional[Dict[str, Any]]:
        """Process a single PR and extract relevant data."""
        try:
            # Extract basic PR info
            author = pr.get("user", {}).get("login", "")
            created_at_str = pr.get("created_at", "")
            merged_at_str = pr.get("merged_at")
            closed_at_str = pr.get("closed_at")

            # Parse timestamps
            created_at = self._parse_timestamp(created_at_str)
            merged_at = self._parse_timestamp(merged_at_str) if merged_at_str else None
            closed_at = self._parse_timestamp(closed_at_str) if closed_at_str else None

            # Apply date filters
            if not self._passes_date_filters(created_at, merged_at, args):
                return None

            # Determine if author is external
            is_team_member = author in team_members

            # Skip team members unless we want to include them for comparison
            if is_team_member:
                return None

            # Calculate lead time metrics
            lead_time_data = self._calculate_lead_time(
                created_at, merged_at, closed_at, args
            )

            # Skip unmerged PRs unless explicitly included
            if not merged_at and not args.include_unmerged:
                return None

            pr_data = {
                "repo": f"{owner}/{repo_name}",
                "pr_number": pr.get("number"),
                "pr_url": pr.get("html_url", ""),
                "title": pr.get("title", ""),
                "author": author,
                "created_at": created_at_str,
                "merged_at": merged_at_str,
                "closed_at": closed_at_str,
                "state": pr.get("state", ""),
                "base_branch": pr.get("base", {}).get("ref", ""),
                "is_team_member": is_team_member,
                **lead_time_data,
            }

            return pr_data

        except Exception as e:
            self.logger.error(
                f"Failed to process PR {pr.get('number', 'unknown')}: {e}"
            )
            return None

    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parse ISO 8601 timestamp string to datetime object."""
        if not timestamp_str:
            return None
        try:
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except Exception:
            return None

    def _passes_date_filters(
        self,
        created_at: Optional[datetime],
        merged_at: Optional[datetime],
        args: Namespace,
    ) -> bool:
        """Check if PR passes date filter criteria."""
        if not args.since and not args.until:
            return True

        # Parse filter dates
        since = self._parse_timestamp(args.since) if args.since else None
        until = self._parse_timestamp(args.until) if args.until else None

        # Choose which timestamp to filter on
        if args.merged_window:
            filter_date = merged_at
        else:
            filter_date = created_at

        if not filter_date:
            return False

        # Apply filters
        if since and filter_date < since:
            return False
        if until and filter_date > until:
            return False

        return True

    def _calculate_lead_time(
        self,
        created_at: Optional[datetime],
        merged_at: Optional[datetime],
        closed_at: Optional[datetime],
        args: Namespace,
    ) -> Dict[str, Any]:
        """Calculate lead time metrics for a PR."""
        lead_time_data: Dict[str, Any] = {
            "lead_time_seconds": None,
            "lead_time_hours": None,
            "lead_time_days": None,
            "is_merged": merged_at is not None,
            "age_seconds": None,
            "age_hours": None,
            "age_days": None,
        }

        try:
            now = datetime.now(timezone.utc)

            if merged_at and created_at:
                # Calculate lead time for merged PRs
                lead_time_delta = merged_at - created_at
                lead_time_seconds = int(lead_time_delta.total_seconds())

                lead_time_data.update(
                    {
                        "lead_time_seconds": lead_time_seconds,
                        "lead_time_hours": round(lead_time_seconds / 3600, 2),
                        "lead_time_days": round(lead_time_seconds / 86400, 2),
                    }
                )

            elif created_at and (not merged_at and args.include_unmerged):
                # Calculate age for unmerged PRs
                age_delta = now - created_at
                age_seconds = int(age_delta.total_seconds())

                lead_time_data.update(
                    {
                        "age_seconds": age_seconds,
                        "age_hours": round(age_seconds / 3600, 2),
                        "age_days": round(age_seconds / 86400, 2),
                    }
                )

        except Exception as e:
            self.logger.error(f"Failed to calculate lead time: {e}")

        return lead_time_data

    def _calculate_metrics(
        self, pr_data: List[Dict[str, Any]], args: Namespace
    ) -> Dict[str, Any]:
        """Calculate summary metrics from PR data."""
        self.logger.info("Calculating summary metrics")

        try:
            # Filter to external PRs only
            external_prs = [pr for pr in pr_data if not pr["is_team_member"]]
            merged_prs = [pr for pr in external_prs if pr["is_merged"]]

            # Extract lead times for merged PRs
            lead_times_days = [
                pr["lead_time_days"]
                for pr in merged_prs
                if pr["lead_time_days"] is not None
            ]
            lead_times_hours = [
                pr["lead_time_hours"]
                for pr in merged_prs
                if pr["lead_time_hours"] is not None
            ]

            metrics: Dict[str, Any] = {
                "total_prs": len(pr_data),
                "external_prs": len(external_prs),
                "merged_external_prs": len(merged_prs),
                "lead_time_metrics": {},
            }

            if lead_times_days:
                metrics["lead_time_metrics"] = {
                    "count": len(lead_times_days),
                    "avg_days": round(statistics.mean(lead_times_days), 2),
                    "median_days": round(statistics.median(lead_times_days), 2),
                    "min_days": round(min(lead_times_days), 2),
                    "max_days": round(max(lead_times_days), 2),
                    "avg_hours": round(statistics.mean(lead_times_hours), 2),
                }

                # Calculate percentiles
                if (
                    len(lead_times_days) >= 10
                ):  # Only calculate percentiles if we have enough data
                    sorted_times = sorted(lead_times_days)
                    n = len(sorted_times)

                    metrics["lead_time_metrics"].update(
                        {
                            "p90_days": round(sorted_times[int(0.9 * n)], 2),
                            "p95_days": round(sorted_times[int(0.95 * n)], 2),
                        }
                    )

            # Repository breakdown
            repo_counts: Dict[str, int] = {}
            for pr in external_prs:
                repo = pr["repo"]
                repo_counts[repo] = repo_counts.get(repo, 0) + 1

            metrics["repository_breakdown"] = dict(
                sorted(repo_counts.items(), key=lambda x: x[1], reverse=True)
            )

            self.logger.info(f"Calculated metrics for {len(external_prs)} external PRs")
            return metrics

        except Exception as e:
            self.logger.error(f"Failed to calculate metrics: {e}")
            raise

    def _generate_outputs(
        self, pr_data: List[Dict[str, Any]], metrics: Dict[str, Any], args: Namespace
    ):
        """Generate output files in specified formats."""
        self.logger.info(f"Generating outputs in {args.format} format(s)")

        try:
            # Ensure output directory exists
            output_dir = os.path.dirname(args.output)
            if output_dir:
                FileManager.create_folder(output_dir)

            # Filter to external PRs
            external_prs = [pr for pr in pr_data if not pr["is_team_member"]]

            # Generate CSV output
            if args.format in ["csv", "both"]:
                self._generate_csv_output(external_prs, args.output)

            # Generate JSON output
            if args.format in ["json", "both"]:
                self._generate_json_output(external_prs, metrics, args.output)

            self.logger.info("Output generation completed")

        except Exception as e:
            self.logger.error(f"Failed to generate outputs: {e}")
            raise

    def _generate_csv_output(self, pr_data: List[Dict[str, Any]], output_prefix: str):
        """Generate CSV output file."""
        csv_file = f"{output_prefix}.csv"

        try:
            if not pr_data:
                self.logger.warning("No PR data to write to CSV")
                return

            # Define CSV columns
            columns = [
                "repo",
                "pr_number",
                "pr_url",
                "title",
                "author",
                "created_at",
                "merged_at",
                "closed_at",
                "state",
                "base_branch",
                "is_merged",
                "lead_time_seconds",
                "lead_time_hours",
                "lead_time_days",
                "age_seconds",
                "age_hours",
                "age_days",
            ]

            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()

                for pr in pr_data:
                    # Create a filtered dict with only the columns we want
                    row = {col: pr.get(col, "") for col in columns}
                    writer.writerow(row)

            self.logger.info(f"CSV output written to: {csv_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate CSV output: {e}")
            raise

    def _generate_json_output(
        self, pr_data: List[Dict[str, Any]], metrics: Dict[str, Any], output_prefix: str
    ):
        """Generate JSON output file."""
        json_file = f"{output_prefix}.json"

        try:
            output_data = {
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "tool": "PyToolkit GitHub PR External Analysis",
                    "version": "1.0",
                },
                "summary": metrics,
                "pull_requests": pr_data,
            }

            JSONManager.write_json(output_data, json_file)
            self.logger.info(f"JSON output written to: {json_file}")

        except Exception as e:
            self.logger.error(f"Failed to generate JSON output: {e}")
            raise

    def _get_output_files(self, args: Namespace) -> List[str]:
        """Get list of generated output files."""
        files = []

        if args.format in ["csv", "both"]:
            files.append(f"{args.output}.csv")

        if args.format in ["json", "both"]:
            files.append(f"{args.output}.json")

        return files
