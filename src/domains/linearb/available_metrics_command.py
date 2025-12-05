"""LinearB Available Metrics Command."""

from argparse import ArgumentParser, Namespace

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_linearb_env_loaded
from utils.logging.logging_manager import LogManager

from .linearb_service import LinearBService


class AvailableMetricsCommand(BaseCommand):
    """Command to list available metrics in LinearB."""

    @staticmethod
    def get_name() -> str:
        return "available-metrics"

    @staticmethod
    def get_description() -> str:
        return "List all available performance metrics in LinearB"

    @staticmethod
    def get_help() -> str:
        return """
List all available performance metrics in LinearB.

This command shows all available metrics that can be retrieved from LinearB,
organized by category (time metrics, PR metrics, commit metrics, etc.).

Examples:
  # List all available metrics
  python src/main.py linearb available-metrics

  # List only time-based metrics
  python src/main.py linearb available-metrics --category time_metrics

  # List metrics with descriptions
  python src/main.py linearb available-metrics --show-descriptions
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--category",
            type=str,
            choices=["time_metrics", "pr_metrics", "commit_metrics", "other_metrics"],
            help="Show metrics for a specific category only",
        )

        parser.add_argument(
            "--show-descriptions",
            action="store_true",
            help="Show detailed descriptions for each metric",
        )

    @staticmethod
    def main(args: Namespace):
        ensure_linearb_env_loaded()
        logger = LogManager.get_instance().get_logger("AvailableMetricsCommand")

        try:
            service = LinearBService()

            logger.info("Available LinearB Performance Metrics")
            logger.info("=" * 60)

            # Get available metrics
            metrics = service.get_available_metrics()

            # Metric descriptions
            descriptions = {
                # Time metrics
                "branch.time_to_pr": "Time from first commit to PR creation",
                "branch.time_to_approve": "Time from PR creation to approval",
                "branch.time_to_merge": "Time from PR approval to merge",
                "branch.time_to_review": "Time from PR creation to first review",
                "branch.review_time": "Total time spent in code review",
                "branch.time_to_prod": "Time from merge to production deployment",
                "branch.computed.cycle_time": "Total cycle time (first commit to production)",
                # PR metrics
                "pr.merged": "Number of merged pull requests",
                "pr.merged.size": "Average size of merged PRs",
                "pr.new": "Number of new pull requests created",
                "pr.review_depth": "Average number of review iterations",
                "pr.reviews": "Total number of code reviews",
                "pr.merged.without.review.count": "Number of PRs merged without review",
                "pr.reviewed": "Number of PRs that received reviews",
                # Commit metrics
                "commit.total.count": "Total number of commits",
                "commit.activity.new_work.count": "Commits for new feature work",
                "commit.activity.rework.count": "Commits for rework/fixes",
                "commit.activity.refactor.count": "Commits for refactoring",
                "commit.total_changes": "Total lines of code changed",
                "commit.activity_days": "Number of active coding days",
                "commit.involved.repos.count": "Number of repositories worked on",
                # Other metrics
                "releases.count": "Number of releases/deployments",
                "branch.state.computed.done": "Number of completed branches",
                "branch.state.active": "Number of active branches",
                "contributor.coding_days": "Number of days with coding activity",
            }

            # Show metrics by category
            categories_to_show = [args.category] if args.category else metrics.keys()

            for category in categories_to_show:
                if category not in metrics:
                    print(f"Category '{category}' not found")
                    continue

                # Format category name
                category_name = category.replace("_", " ").title()
                print(f"\n{category_name}:")
                print("-" * len(category_name + ":"))

                for i, metric in enumerate(metrics[category], 1):
                    if args.show_descriptions and metric in descriptions:
                        print(f"{i:2d}. {metric}")
                        print(f"     {descriptions[metric]}")
                    else:
                        print(f"{i:2d}. {metric}")

            # Show usage information
            print("\n" + "=" * 60)
            print("Usage Information:")
            print("- These metrics can be used with the performance-metrics command")
            print("- Time-based metrics support aggregations: p75, avg, p50")
            print("- Count-based metrics use default aggregation")
            print("- Use --aggregation parameter to specify aggregation type")

            print("\nExample:")
            print("python src/main.py linearb performance-metrics \\")
            print("  --time-range last-week \\")
            print("  --aggregation p75 \\")
            print("  --filter-type team")

            print("Available metrics listing completed successfully")

        except Exception as e:
            logger.error(f"Available metrics command failed: {e}")
            exit(1)
