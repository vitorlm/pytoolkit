"""
GitHub Pull Request Analysis Command

Analyzes PRs from all contributors (both internal and external) and computes lead time metrics.
Can optionally compute review rounds and filter by contributor type.
"""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_github_env_loaded
from utils.error.error_manager import handle_generic_exception
from utils.logging.logging_manager import LogManager

from .pr_analysis_service import PrAnalysisService


class PrAnalysisCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "pr-analysis"

    @staticmethod
    def get_description() -> str:
        return "Analyze PRs from contributors (both internal and external) with lead time metrics"

    @staticmethod
    def get_help() -> str:
        return """
                Analyze GitHub Pull Requests from all contributors with lead time metrics.
                Can filter to external-only or internal-only contributors.

                NEW: Enhanced approver analysis via GraphQL
                - approvers: List of unique reviewers who approved the PR
                - approvers_count: Number of unique approvers
                - latest_approvals: Latest approval per reviewer with timestamps
                - review_decision: GitHub's review decision (APPROVED/CHANGES_REQUESTED/REVIEW_REQUIRED)
                - approvals_valid_now: Heuristic count of approvals still considered valid
                - approvals_after_last_push: Approvals submitted after the last commit

                Examples:
                # Analyze all PRs (internal + external) with enhanced approver data
                python src/main.py github pr-analysis --org syngenta-digital --repos repo1 repo2 --include-approvers
                
                # Analyze external-only PRs with custom teams and enhanced approval metrics
                python src/main.py github pr-analysis \\
                    --org syngenta-digital --all-repos --external-only \\
                    --codeowners-teams @syngenta-digital/team1 @syngenta-digital/team2 \\
                    --since 2024-01-01T00:00:00Z --until 2024-12-31T23:59:59Z \\
                    --include-approvers --approvals-heuristics
                
                # Analyze internal-only PRs with basic metrics (no approver details)
                python src/main.py github pr-analysis \\
                    --org syngenta-digital --repos my-repo --internal-only \\
                    --format csv --verbose --output ./custom-output \\
                    --no-approvers
                
                # High-performance analysis with custom GraphQL page size
                python src/main.py github pr-analysis \\
                    --org syngenta-digital --all-repos \\
                    --use-graphql --graphql-page-size 100 \\
                    --include-approvers

                Setup:
                1. Add your GitHub token to .env file:
                    GITHUB_TOKEN=your_github_token_here
                
                2. Get your token from: https://github.com/settings/tokens
                    Required scopes: repo, read:org
            """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--org",
            required=True,
            help="GitHub organization name (e.g., 'syngenta-digital')",
        )

        # Repository selection (mutually exclusive)
        repo_group = parser.add_mutually_exclusive_group(required=True)
        repo_group.add_argument(
            "--repos", nargs="+", help="One or more repository names within the org"
        )
        repo_group.add_argument(
            "--all-repos",
            action="store_true",
            help="Process all repositories in the organization",
        )

        parser.add_argument(
            "--include-archived",
            action="store_true",
            help="Include archived repositories when using --all-repos",
        )

        # Date filtering
        parser.add_argument(
            "--since",
            help="Filter PRs created after this ISO 8601 datetime (inclusive)",
        )
        parser.add_argument(
            "--until",
            help="Filter PRs created before this ISO 8601 datetime (inclusive)",
        )

        filter_group = parser.add_mutually_exclusive_group()
        filter_group.add_argument(
            "--created-window",
            action="store_true",
            default=True,
            help="Apply date filters to PR created_at (default)",
        )
        filter_group.add_argument(
            "--merged-window",
            action="store_true",
            help="Apply date filters to PR merged_at instead of created_at",
        )

        parser.add_argument(
            "--state",
            choices=["open", "closed", "all"],
            default="all",
            help="PR state to analyze (default: all)",
        )

        # CODEOWNERS teams
        parser.add_argument(
            "--codeowners-teams",
            nargs="*",
            default=[
                "@syngenta-digital/cropwise-core-services-catalog",
                "@syngenta-digital/cropwise-core-services-identity",
                "@syngenta-digital/cropwise-core-services-da-backbone",
            ],
            help="Team names in @org/team format. Default: Syngenta Digital core services teams",
        )

        # Output options
        parser.add_argument(
            "--output-dir",
            default="./output",
            help="Output directory for all artifacts (default: ./output)",
        )
        parser.add_argument(
            "--output",
            default="github_prs",
            help="Output file prefix within output-dir (default: github_prs)",
        )
        parser.add_argument(
            "--format",
            choices=["csv", "json", "both"],
            default="both",
            help="Output format (default: both)",
        )

        # Processing options
        parser.add_argument(
            "--include-unmerged",
            action="store_true",
            help="Include unmerged PRs in analysis (compute age instead of lead time)",
        )
        parser.add_argument(
            "--max-workers",
            type=int,
            default=6,
            help="Maximum concurrent API workers (default: 6)",
        )

        # GraphQL acceleration flags
        parser.add_argument(
            "--use-graphql",
            action="store_true",
            default=True,
            help="Enable GraphQL fast path for fetching PRs (default: enabled; much faster than REST)",
        )
        parser.add_argument(
            "--use-rest",
            dest="use_graphql",
            action="store_false",
            help="Force use of REST API instead of GraphQL (slower but compatible with older GitHub versions)",
        )
        parser.add_argument(
            "--graphql-page-size",
            type=int,
            default=100,
            help="GraphQL page size (50-100 recommended, default: 100)",
        )
        parser.add_argument(
            "--review-rounds-mode",
            choices=["heuristic", "rest"],
            default="heuristic",
            help="Method for computing review_rounds: 'heuristic' (fast, GraphQL commits delta) or 'rest' (exact, uses REST timeline)",
        )
        parser.add_argument(
            "--no-graphql-fallback-rest",
            action="store_true",
            help="Disable automatic REST fallback if GraphQL returns zero PRs",
        )

        # New approver-specific flags
        parser.add_argument(
            "--include-approvers",
            action="store_true",
            default=True,
            help="Include detailed approver data (approvers, latest_approvals, review_decision) via enhanced GraphQL (default: enabled)",
        )
        parser.add_argument(
            "--no-approvers",
            dest="include_approvers",
            action="store_false",
            help="Disable approver data collection to use basic GraphQL query (faster but less detailed)",
        )
        parser.add_argument(
            "--approvals-heuristics",
            action="store_true",
            default=True,
            help="Calculate approval heuristics (approvals_valid_now, approvals_after_last_push) (default: enabled)",
        )
        parser.add_argument(
            "--no-approvals-heuristics",
            dest="approvals_heuristics",
            action="store_false",
            help="Disable approval heuristics calculation",
        )

        # Enrichment options
        parser.add_argument(
            "--include-size-metrics",
            action="store_true",
            default=True,
            help="Include PR size metrics (additions, deletions, changed_files, commits)",
        )
        parser.add_argument(
            "--no-size-metrics",
            dest="include_size_metrics",
            action="store_false",
            help="Disable PR size metrics collection",
        )
        parser.add_argument(
            "--include-review-metrics",
            action="store_true",
            default=True,
            help="Include PR review metrics (reviews, approvals, comments, response times)",
        )
        parser.add_argument(
            "--no-review-metrics",
            dest="include_review_metrics",
            action="store_false",
            help="Disable PR review metrics collection",
        )

        parser.add_argument(
            "--include-review-rounds",
            action="store_true",
            help="Include review rounds calculation (requires additional API calls for timeline and commits)",
        )

        # Filtering options
        filter_group = parser.add_mutually_exclusive_group()
        filter_group.add_argument(
            "--external-only",
            action="store_true",
            help="Include only external PRs (authors not in CODEOWNERS teams) in output",
        )
        filter_group.add_argument(
            "--internal-only",
            action="store_true",
            help="Include only internal PRs (authors in CODEOWNERS teams) in output",
        )

        # Verbose logging
        parser.add_argument(
            "--verbose", action="store_true", help="Enable verbose (DEBUG) logging"
        )

    @staticmethod
    def main(args: Namespace):
        # ALWAYS start with environment loading
        ensure_github_env_loaded()

        # Get logger with component name
        logger = LogManager.get_instance().get_logger("PrAnalysisCommand")

        try:
            service = PrAnalysisService()

            logger.info(f"Starting PR analysis for org: {args.org}")
            logger.info(f"CODEOWNERS teams: {args.codeowners_teams}")

            if args.external_only:
                logger.info("Filter: External PRs only")
            elif args.internal_only:
                logger.info("Filter: Internal PRs only")
            else:
                logger.info("Filter: All PRs (internal + external)")

            # Execute analysis
            results = service.analyze_prs(args)

            logger.info(
                f"Analysis completed successfully. Processed {results.get('total_prs', 0)} PRs"
            )
            logger.info(f"External PRs found: {results.get('external_prs', 0)}")
            logger.info(f"Output saved to: {args.output}")

        except Exception as e:
            logger.error(f"PR analysis failed: {e}")
            handle_generic_exception(e, "PR analysis", {"org": args.org})
            exit(1)
