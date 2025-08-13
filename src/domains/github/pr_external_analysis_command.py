"""
GitHub Pull Request External Contributors Analysis Command

Analyzes PRs authored by contributors who are NOT members of specified CODEOWNERS teams
and computes lead time metrics from PR creation to merge.
"""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_github_env_loaded
from utils.error.error_manager import handle_generic_exception
from utils.logging.logging_manager import LogManager

from .pr_analysis_service import PrAnalysisService


class PrExternalAnalysisCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "pr-external-analysis"

    @staticmethod
    def get_description() -> str:
        return "Analyze PRs from external contributors (not in CODEOWNERS teams) with lead time metrics"

    @staticmethod
    def get_help() -> str:
        return """
                Analyze GitHub Pull Requests authored by contributors who are NOT members of 
                specified CODEOWNERS teams, computing lead time from PR creation to merge.

                Examples:
                # Analyze specific repos with default CODEOWNERS teams
                python src/main.py github pr-external-analysis --org syngenta-digital --repos repo1 repo2
                
                # Analyze all repos with custom teams and date range
                python src/main.py github pr-external-analysis \\
                    --org syngenta-digital --all-repos \\
                    --codeowners-teams @syngenta-digital/team1 @syngenta-digital/team2 \\
                    --since 2024-01-01T00:00:00Z --until 2024-12-31T23:59:59Z
                
                # Output only CSV format with verbose logging
                python src/main.py github pr-external-analysis \\
                    --org syngenta-digital --repos my-repo \\
                    --format csv --verbose --output ./custom-output

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
        repo_group.add_argument("--repos", nargs="+", help="One or more repository names within the org")
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
            default="github_external_prs",
            help="Output file prefix within output-dir (default: github_external_prs)",
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

        # Verbose logging
        parser.add_argument("--verbose", action="store_true", help="Enable verbose (DEBUG) logging")

    @staticmethod
    def main(args: Namespace):
        # ALWAYS start with environment loading
        ensure_github_env_loaded()

        # Get logger with component name
        logger = LogManager.get_instance().get_logger("PrExternalAnalysisCommand")

        try:
            service = PrAnalysisService()

            logger.info(f"Starting PR external analysis for org: {args.org}")
            logger.info(f"CODEOWNERS teams: {args.codeowners_teams}")

            # Execute analysis
            results = service.analyze_external_prs(args)

            logger.info(f"Analysis completed successfully. Processed {results.get('total_prs', 0)} PRs")
            logger.info(f"External PRs found: {results.get('external_prs', 0)}")
            logger.info(f"Output saved to: {args.output}")

        except Exception as e:
            logger.error(f"PR analysis failed: {e}")
            handle_generic_exception(e, "PR external analysis", {"org": args.org})
            exit(1)
