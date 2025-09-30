"""
GitHub Pull Request Analysis Service

Business logic for analyzing PRs from all contributors (both internal and external) and computing metrics.
"""

import csv
import os
import statistics
import time
from argparse import Namespace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from utils.cache_manager.cache_manager import CacheManager
from utils.data.json_manager import JSONManager
from utils.error.error_manager import handle_generic_exception
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager

try:
    from .github_api_client import GitHubApiClient  # type: ignore
except ImportError:  # Fallback if executed as a script
    from domains.github.github_api_client import GitHubApiClient  # type: ignore


class PrAnalysisService:
    """Service for analyzing PRs from all contributors (both internal and external) and computing lead time metrics."""

    def __init__(self, max_workers: int = 4):
        self.logger = LogManager.get_instance().get_logger("PrAnalysisService")
        self.cache = CacheManager.get_instance()
        self.github_client = GitHubApiClient(max_workers=max_workers)

    def analyze_prs(self, args: Namespace) -> Dict[str, Any]:
        """
        Main method to analyze PRs from all contributors (both internal and external).

        Args:
            args: Command line arguments from ArgumentParser

        Returns:
            Dictionary with analysis results and metadata
        """
        try:
            self.logger.info("Starting PR analysis")

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
                "external_prs": len(
                    [pr for pr in pr_data if pr.get("is_external", False)]
                ),
                "team_members_count": len(team_members),
                "repositories_analyzed": len(repos),
                "metrics": metrics,
                "output_files": self._get_output_files(args),
            }

            self.logger.info("PR analysis completed successfully")
            return results

        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
            handle_generic_exception(e, "PR analysis", {"args": str(args)})
            raise

    def _resolve_team_members(self, team_specs: List[str]) -> set:
        """Resolve team members from CODEOWNERS team specifications."""
        self.logger.info(
            f"👥 Resolving team members from {len(team_specs)} teams: {team_specs}"
        )

        try:
            team_members = self.github_client.resolve_team_members_parallel(team_specs)
            self.logger.info(f"✅ Resolved {len(team_members)} unique team members")
            return team_members
        except Exception as e:
            self.logger.error(f"❌ Failed to resolve team members: {e}")
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
        self.logger.info(f"📊 Collecting PR data from {len(repos)} repositories")

        all_pr_data = []

        for i, repo in enumerate(repos, 1):
            try:
                repo_name = repo["name"]
                owner = repo["owner"]["login"] if "owner" in repo else args.org

                self.logger.info(
                    f"📂 Processing repository {i}/{len(repos)}: {owner}/{repo_name}"
                )

                # Fetch PRs via selected API path
                if getattr(args, "use_graphql", False):
                    self.logger.info(
                        f"⚡ Using optimized GraphQL path (page_size={getattr(args, 'graphql_page_size', 50)}) for {owner}/{repo_name}"
                    )
                    self.logger.info(
                        "🔥 This will fetch ALL PR data in 1-2 API calls instead of hundreds!"
                    )

                    # Use enhanced GraphQL method with approvers if requested
                    if getattr(args, "include_approvers", True):
                        self.logger.info("📊 Including approver data in GraphQL query")
                        prs = (
                            self.github_client.fetch_pull_requests_graphql_with_reviews(
                                owner,
                                repo_name,
                                args.since
                                if getattr(args, "merged_window", False) is False
                                else None,
                                getattr(args, "until", None),
                                getattr(args, "graphql_page_size", 50),
                            )
                        )
                    else:
                        # Use basic GraphQL method without enhanced approver data
                        prs = self.github_client.get_enriched_pull_requests_graphql(
                            owner,
                            repo_name,
                            args.since
                            if getattr(args, "merged_window", False) is False
                            else None,
                            getattr(args, "graphql_page_size", 50),
                        )
                else:
                    self.logger.info(
                        f"🐌 Using slower REST API for {owner}/{repo_name} (consider --use-graphql)"
                    )
                    prs = self.github_client.get_pull_requests(
                        owner, repo_name, args.state
                    )
                self.logger.info(f"✅ Fetched {len(prs)} PRs in {owner}/{repo_name}")

                if not prs:
                    continue

                # Step 1: Apply date and merge filters before enrichment
                filtered_prs = []

                self.logger.info(
                    f"Pre-filtering {len(prs)} PRs with date and merge criteria..."
                )

                for i, pr in enumerate(prs, 1):
                    if i % 100 == 0 or i == len(prs):
                        self.logger.info(
                            f"Pre-filtering progress: {i}/{len(prs)} PRs processed"
                        )

                    # Apply date filters early (before expensive API calls)
                    created_at_str = pr.get("created_at", "")
                    merged_at_str = pr.get("merged_at")

                    created_at = self._parse_timestamp(created_at_str)
                    merged_at = (
                        self._parse_timestamp(merged_at_str) if merged_at_str else None
                    )

                    if not self._passes_date_filters(created_at, merged_at, args):
                        continue

                    # Skip unmerged PRs unless explicitly included
                    if not merged_at and not args.include_unmerged:
                        continue

                    filtered_prs.append(pr)

                self.logger.info(
                    f"After pre-filtering: {len(filtered_prs)} PRs need processing (reduced from {len(prs)} total PRs)"
                )

                if not filtered_prs:
                    self.logger.info(
                        f"No PRs found in {owner}/{repo_name} after filtering"
                    )
                    continue

                # Step 2: Process filtered PRs with enrichment and progress tracking
                # Note: if using GraphQL, PRs are already enriched
                is_graphql = getattr(args, "use_graphql", False)
                repo_pr_data = self._process_filtered_prs(
                    filtered_prs, owner, repo_name, team_members, args, is_graphql
                )

                all_pr_data.extend(repo_pr_data)
                self.logger.info(
                    f"Completed processing {owner}/{repo_name}: {len(repo_pr_data)} PRs added"
                )

            except Exception as e:
                self.logger.error(
                    f"Failed to process repository {repo.get('name', 'unknown')}: {e}"
                )
                continue

        self.logger.info(
            f"Collected data for {len(all_pr_data)} PRs across all repositories"
        )
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

            # Determine if author is external (not in CODEOWNERS teams)
            author_normalized = author.lower()
            team_members_normalized = {member.lower() for member in team_members}
            is_external = author_normalized not in team_members_normalized
            is_team_member = not is_external  # Keep for backward compatibility

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
                "is_external": is_external,
                **lead_time_data,
                # Size metrics
                "additions": pr.get("additions"),
                "deletions": pr.get("deletions"),
                "changed_files": pr.get("changed_files"),
                "commits": pr.get("commits"),
                # Review and discussion metrics
                "reviews_count": pr.get("reviews_count"),
                "approvals_count": pr.get("approvals_count"),
                "review_comments": pr.get("review_comments"),
                "issue_comments": pr.get("issue_comments"),
                "requested_reviewers_count": pr.get("requested_reviewers_count"),
                "requested_reviewers": pr.get("requested_reviewers"),
                "requested_teams": pr.get("requested_teams"),
                "time_to_first_review_seconds": pr.get("time_to_first_review_seconds"),
                "first_response_latency_seconds": pr.get(
                    "first_response_latency_seconds"
                ),
                # Review rounds metrics
                "review_rounds": pr.get("review_rounds"),
                "synchronize_after_first_review": pr.get(
                    "synchronize_after_first_review"
                ),
                "re_review_pushes": pr.get("re_review_pushes"),
                # Approver fields (added by GraphQL with reviews)
                "approvers": pr.get("approvers"),
                "approvers_count": pr.get("approvers_count"),
                "latest_approvals": pr.get("latest_approvals"),
                "review_decision": pr.get("review_decision"),
                "approvals_valid_now": pr.get("approvals_valid_now"),
                "approvals_after_last_push": pr.get("approvals_after_last_push"),
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

    def _process_filtered_prs(
        self,
        filtered_prs: List[Dict[str, Any]],
        owner: str,
        repo_name: str,
        team_members: set,
        args: Namespace,
        is_graphql: bool = False,
    ) -> List[Dict[str, Any]]:
        """Process pre-filtered PRs with enrichment and progress tracking."""

        processed_prs = []
        total_prs = len(filtered_prs)

        # Determine if we need enrichment (skip if GraphQL already provided enriched data)
        needs_enrichment = not is_graphql and (
            args.include_size_metrics
            or args.include_review_metrics
            or getattr(args, "include_review_rounds", False)
        )

        if is_graphql:
            self.logger.info(
                f"Processing {total_prs} PRs with GraphQL enriched data (fast mode)..."
            )
            # GraphQL data is already enriched, just process
            for i, pr in enumerate(filtered_prs, 1):
                try:
                    if i % 50 == 0 or i == total_prs:
                        self.logger.info(
                            f"Processing progress: {i}/{total_prs} PRs ({(i / total_prs) * 100:.1f}%)"
                        )

                    pr_data = self._process_single_pr(
                        pr, owner, repo_name, team_members, args
                    )

                    if pr_data:
                        processed_prs.append(pr_data)

                except Exception as e:
                    self.logger.error(
                        f"Failed to process PR {pr.get('number', 'unknown')}: {e}"
                    )
                    continue
        elif needs_enrichment:
            self.logger.info(
                f"Starting enrichment for {total_prs} PRs in {owner}/{repo_name}..."
            )
            self.logger.info(
                f"Each PR requires 1-6 API calls. Estimated time: {total_prs * 2}-{total_prs * 12} seconds"
            )

            # Track timing for progress estimation
            start_time = time.time()

            # Process PRs with progress tracking
            for i, pr in enumerate(filtered_prs, 1):
                try:
                    # Progress logging every 10 PRs or at key milestones
                    if i % 10 == 0 or i == total_prs or i in [1, 5, 25, 50, 100]:
                        elapsed = time.time() - start_time
                        rate = i / elapsed if elapsed > 0 else 0
                        remaining = total_prs - i
                        eta_seconds = remaining / rate if rate > 0 else 0

                        self.logger.info(
                            f"Enrichment progress: {i}/{total_prs} PRs ({(i / total_prs) * 100:.1f}%) | "
                            f"Rate: {rate:.2f} PRs/sec | ETA: {eta_seconds / 60:.1f} min"
                        )

                    # Enrich PR with additional metadata
                    enriched_pr = self.github_client.enrich_pull_request(
                        owner,
                        repo_name,
                        pr,
                        args.include_size_metrics,
                        args.include_review_metrics,
                        getattr(args, "include_review_rounds", False),
                    )

                    pr_data = self._process_single_pr(
                        enriched_pr, owner, repo_name, team_members, args
                    )

                    if pr_data:
                        processed_prs.append(pr_data)

                except Exception as e:
                    self.logger.error(
                        f"Failed to process PR {pr.get('number', 'unknown')}: {e}"
                    )
                    continue

            total_time = time.time() - start_time
            self.logger.info(
                f"Enrichment completed in {total_time / 60:.2f} minutes ({total_time / total_prs:.2f}s per PR)"
            )
        else:
            # No enrichment needed - process quickly
            self.logger.info(
                f"Processing {total_prs} PRs without enrichment (fast mode)..."
            )

            for i, pr in enumerate(filtered_prs, 1):
                try:
                    if i % 50 == 0 or i == total_prs:
                        self.logger.info(
                            f"Processing progress: {i}/{total_prs} PRs ({(i / total_prs) * 100:.1f}%)"
                        )

                    pr_data = self._process_single_pr(
                        pr, owner, repo_name, team_members, args
                    )

                    if pr_data:
                        processed_prs.append(pr_data)

                except Exception as e:
                    self.logger.error(
                        f"Failed to process PR {pr.get('number', 'unknown')}: {e}"
                    )
                    continue

        return processed_prs

    def _calculate_metrics(
        self, pr_data: List[Dict[str, Any]], args: Namespace
    ) -> Dict[str, Any]:
        """Calculate summary metrics from PR data."""
        self.logger.info("Calculating summary metrics")

        try:
            # Separate internal and external PRs
            external_prs = [pr for pr in pr_data if pr.get("is_external", False)]
            internal_prs = [pr for pr in pr_data if not pr.get("is_external", False)]

            # Focus metrics on merged PRs
            merged_external_prs = [pr for pr in external_prs if pr["is_merged"]]
            merged_internal_prs = [pr for pr in internal_prs if pr["is_merged"]]

            # Extract lead times for merged external PRs (primary focus)
            lead_times_days = [
                pr["lead_time_days"]
                for pr in merged_external_prs
                if pr["lead_time_days"] is not None
            ]
            lead_times_hours = [
                pr["lead_time_hours"]
                for pr in merged_external_prs
                if pr["lead_time_hours"] is not None
            ]

            metrics: Dict[str, Any] = {
                "total_prs": len(pr_data),
                "external_prs": len(external_prs),
                "internal_prs": len(internal_prs),
                "merged_external_prs": len(merged_external_prs),
                "merged_internal_prs": len(merged_internal_prs),
                "external_merge_rate": round(
                    len(merged_external_prs) / len(external_prs) * 100, 2
                )
                if external_prs
                else 0,
                "internal_merge_rate": round(
                    len(merged_internal_prs) / len(internal_prs) * 100, 2
                )
                if internal_prs
                else 0,
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
            FileManager.create_folder(args.output_dir)

            # Build full output path
            output_path = os.path.join(args.output_dir, args.output)

            # Apply external-only filter if specified in args
            output_prs = pr_data
            if getattr(args, "external_only", False):
                output_prs = [pr for pr in pr_data if pr.get("is_external", False)]
            elif getattr(args, "internal_only", False):
                output_prs = [pr for pr in pr_data if not pr.get("is_external", False)]

            # Generate CSV output
            if args.format in ["csv", "both"]:
                self._generate_csv_output(output_prs, output_path)

            # Generate JSON output
            if args.format in ["json", "both"]:
                self._generate_json_output(output_prs, metrics, output_path)

            self.logger.info(f"Output generation completed in {args.output_dir}")

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
                "is_team_member",
                "is_external",
                "is_merged",
                "lead_time_seconds",
                "lead_time_hours",
                "lead_time_days",
                "age_seconds",
                "age_hours",
                "age_days",
                # Size metrics
                "additions",
                "deletions",
                "changed_files",
                "commits",
                # Review and discussion metrics
                "reviews_count",
                "approvals_count",
                "review_comments",
                "issue_comments",
                "requested_reviewers_count",
                "requested_reviewers",
                "requested_teams",
                "time_to_first_review_seconds",
                "first_response_latency_seconds",
                # Review rounds metrics
                "review_rounds",
                "synchronize_after_first_review",
                "re_review_pushes",
                # New approver fields
                "approvers",
                "approvers_count",
                "latest_approvals",
                "review_decision",
                "approvals_valid_now",
                "approvals_after_last_push",
            ]

            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()

                for pr in pr_data:
                    # Create a filtered dict with only the columns we want
                    row = {}
                    for col in columns:
                        value = pr.get(col, "")
                        # Handle list/array fields by converting to JSON string
                        if col in [
                            "requested_reviewers",
                            "requested_teams",
                            "approvers",
                        ] and isinstance(value, list):
                            row[col] = ",".join(value) if value else ""
                        elif col == "latest_approvals" and isinstance(value, list):
                            # Convert latest_approvals to a more readable format
                            row[col] = (
                                "; ".join(
                                    [
                                        f"{a.get('login', '')}@{a.get('submitted_at', '')}"
                                        for a in value
                                    ]
                                )
                                if value
                                else ""
                            )
                        else:
                            row[col] = value
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
        output_path = os.path.join(args.output_dir, args.output)

        if args.format in ["csv", "both"]:
            files.append(f"{output_path}.csv")

        if args.format in ["json", "both"]:
            files.append(f"{output_path}.json")

        return files
