"""
GitHub Pull Request Workload Analysis Command

Analyzes PR data from JSON/CSV files to evaluate CODEOWNERS workload and assess if they
are under pressure due to increased PRs from external authors.
"""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.error.error_manager import handle_generic_exception
from utils.logging.logging_manager import LogManager

from .pr_workload_analysis_service import PrWorkloadAnalysisService


class PrWorkloadAnalysisCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "pr-workload-analysis"

    @staticmethod
    def get_description() -> str:
        return "Analyze PR data from JSON/CSV files to evaluate CODEOWNERS workload and pressure"

    @staticmethod
    def get_help() -> str:
        return """
        Analyze Pull Request data from JSON or CSV files to evaluate whether CODEOWNERS 
        are under pressure due to increased PRs from external authors.

        Expected file format (JSON or CSV) with fields:
        - author: PR author username
        - created_at: PR creation timestamp
        - merged_at: PR merge timestamp (if merged)
        - closed_at: PR close timestamp (if closed)
        - is_team_member: Boolean indicating if author is a CODEOWNER
        - lead_time_days: Days from creation to merge/close
        - additions: Lines of code added
        - deletions: Lines of code deleted
        - changed_files: Number of files changed
        - commits: Number of commits
        - reviews_count: Number of reviews
        - review_comments: Number of review comments
        - approvals_count: Number of approvals
        - requested_reviewers_count: Number of requested reviewers

        Analyses performed:
        - Monthly trends for PR counts and lead times (work-days only)
        - Correlation between PR size and lead time
        - CODEOWNERS workload pressure metrics with work-day calculations
        - External PR workload intensity (excludes weekends)
        - Recommendations for workload management

        Examples:
        # Analyze PR data from JSON file
        python src/main.py github pr-workload-analysis --file data/pr_data.json

        # Analyze PR data from CSV file with custom output directory
        python src/main.py github pr-workload-analysis --file data/pr_data.csv --output output/custom

        # Include detailed correlation analysis
        python src/main.py github pr-workload-analysis --file data/pr_data.json --detailed-analysis

        # Generate monthly trend charts
        python src/main.py github pr-workload-analysis --file data/pr_data.json --generate-charts

        # Analyze PRs from a specific date range (work-days only calculations)
        python src/main.py github pr-workload-analysis --file data/pr_data.json --start-date 2024-01-01 --end-date 2024-12-31

        # Analyze PRs from start date to current date
        python src/main.py github pr-workload-analysis --file data/pr_data.json --start-date 2024-06-01

        # Analyze with custom team size
        python src/main.py github pr-workload-analysis --file data/pr_data.json --team-size 8

        Output files (saved to output/ folder):
        - pr_workload_summary.json: Complete analysis results
        - monthly_trends.csv: Monthly PR counts and lead time trends
        - correlation_matrix.csv: Correlation analysis between metrics
        - codeowners_pressure_metrics.csv: CODEOWNERS workload metrics
        - recommendations.txt: Insights and recommendations
        - charts/: Visualization charts (if --generate-charts is used)
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        """Define command line arguments."""
        parser.add_argument("--file", required=True, help="Path to JSON or CSV file containing PR data")

        parser.add_argument(
            "--output", default="output", help="Output directory for analysis results (default: output)"
        )

        parser.add_argument(
            "--detailed-analysis",
            action="store_true",
            help="Include detailed correlation analysis and additional metrics",
        )

        parser.add_argument(
            "--generate-charts", action="store_true", help="Generate visualization charts for trends and correlations"
        )

        parser.add_argument(
            "--date-format",
            default="%Y-%m-%dT%H:%M:%SZ",
            help="Date format for timestamp parsing (default: %%Y-%%m-%%dT%%H:%%M:%%SZ)",
        )

        parser.add_argument(
            "--min-records", type=int, default=10, help="Minimum number of records required for analysis (default: 10)"
        )

        parser.add_argument(
            "--start-date",
            help="Filter PRs created on or after this date (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS)"
        )

        parser.add_argument(
            "--end-date", 
            help="Filter PRs created on or before this date (format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS). If start-date is provided and end-date is not, current date is used"
        )

        parser.add_argument(
            "--team-size",
            type=int,
            default=6,
            help="Number of engineers on the team who can review PRs (default: 6)"
        )

    @staticmethod
    def main(args: Namespace):
        """Main command execution."""
        # ALWAYS start with environment loading
        ensure_env_loaded()

        # Get logger with component name
        logger = LogManager.get_instance().get_logger("PrWorkloadAnalysisCommand")

        try:
            logger.info("Starting PR workload analysis")
            logger.info(f"Input file: {args.file}")
            logger.info(f"Output directory: {args.output}")

            # Create service instance and execute analysis
            service = PrWorkloadAnalysisService()
            results = service.analyze_pr_workload(args)

            # Log summary of results
            logger.info("PR workload analysis completed successfully")
            logger.info(f"Total PRs analyzed: {results.get('total_prs', 0)}")
            logger.info(f"External author PRs: {results.get('external_prs', 0)}")
            logger.info(f"Analysis period: {results.get('analysis_period', 'N/A')}")
            logger.info(f"Output files generated: {len(results.get('output_files', []))}")

            # Print key insights
            if "key_insights" in results:
                logger.info("Key insights:")
                for insight in results["key_insights"][:3]:  # Show top 3 insights
                    logger.info(f"  - {insight}")

        except Exception as e:
            logger.error(f"PR workload analysis failed: {e}")
            handle_generic_exception(e, "PR workload analysis", {"file": args.file})
            exit(1)
