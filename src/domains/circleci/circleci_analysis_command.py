"""CircleCI Performance Analysis Command
Command interface for CircleCI pipeline performance analysis
"""

import os
from argparse import ArgumentParser

from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded, load_domain_env
from utils.logging.logging_manager import LogManager

from .circleci_service import CircleCIService


class CircleCIAnalysisCommand(BaseCommand):
    """Command for analyzing CircleCI pipeline performance"""

    @staticmethod
    def get_name() -> str:
        return "circleci-analyze"

    @staticmethod
    def get_description() -> str:
        return "Analyze CircleCI pipeline performance and generate optimization recommendations"

    @staticmethod
    def get_help() -> str:
        return """
Analyze CircleCI pipeline performance and generate optimization recommendations.

This command analyzes CircleCI pipelines, workflows, and jobs to identify
performance bottlenecks and provide actionable optimization recommendations.

Examples:
  # List all available projects
  python src/main.py circleci circleci-analyze --list-projects

  # Basic analysis with summary
  python src/main.py circleci circleci-analyze --project-slug gh/org/repo --summary-only

  # Complete analysis with charts
  python src/main.py circleci circleci-analyze --project-slug gh/org/repo --output-dir ./analysis

  # Limited analysis for faster results
  python src/main.py circleci circleci-analyze --project-slug gh/org/repo --pipeline-limit 50

Setup:
  1. Add your CircleCI token to .env file:
     CIRCLECI_TOKEN=your_token_here
  
  2. Get your token from: https://app.circleci.com/settings/user/tokens
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        """Configure command line arguments"""
        parser.add_argument(
            "--token",
            type=str,
            help="CircleCI API token (or set CIRCLECI_TOKEN env var)",
        )
        parser.add_argument("--project-slug", type=str, help="Project slug (e.g., gh/org/repo)")
        parser.add_argument(
            "--list-projects",
            action="store_true",
            help="List all available projects and their slugs",
        )
        parser.add_argument(
            "--output-dir",
            type=str,
            default="./circleci-analysis",
            help="Output directory for analysis results (default: ./circleci-analysis)",
        )
        parser.add_argument(
            "--pipeline-limit",
            type=int,
            default=100,
            help="Maximum number of pipelines to analyze (default: 100)",
        )
        parser.add_argument(
            "--workflow-limit",
            type=int,
            default=20,
            help="Maximum number of workflows to analyze per pipeline (default: 20)",
        )
        parser.add_argument(
            "--job-limit",
            type=int,
            default=10,
            help="Maximum number of jobs to analyze per workflow (default: 10)",
        )
        parser.add_argument(
            "--no-charts",
            action="store_true",
            help="Skip generating performance charts",
        )
        parser.add_argument(
            "--summary-only",
            action="store_true",
            help="Show only summary information without detailed analysis",
        )

    @staticmethod
    def main(args):
        """Execute CircleCI analysis command"""
        ensure_env_loaded()
        load_domain_env("domains/circleci")
        logger = LogManager.get_instance().get_logger("CircleCIAnalysisCommand")

        # Debug: Check if token is loaded
        token_from_env = os.getenv("CIRCLECI_TOKEN")
        logger.info(f"Token from environment: {'***FOUND***' if token_from_env else 'NOT FOUND'}")

        # Get CircleCI token
        token = args.token or token_from_env
        if not token:
            logger.error("❌ CircleCI token required. Use --token or set CIRCLECI_TOKEN env var")
            logger.info("   Get token from: https://app.circleci.com/settings/user/tokens")
            exit(1)

        logger.info("✅ CircleCI token found, proceeding with API calls...")

        try:
            # Initialize service
            service = CircleCIService(token, args.project_slug if args.project_slug else "")

            # Handle list projects option
            if args.list_projects:
                CircleCIAnalysisCommand._list_projects(service)
                return

            # Validate project slug is provided for analysis
            if not args.project_slug:
                logger.error(
                    "❌ Project slug is required for analysis. Use --project-slug or --list-projects to see available projects"
                )
                exit(1)

            if args.summary_only:
                # Quick analysis
                pipelines = service.export_pipelines(limit=20)
                workflows = service.export_workflows(pipelines, limit=10)
                jobs = service.export_jobs(workflows, limit=5)
                analysis = service.generate_analysis(pipelines, workflows, jobs)

                CircleCIAnalysisCommand._print_summary(analysis)
                logger.info("Analysis completed successfully")

            else:
                # Complete analysis
                result = service.run_complete_analysis(args.output_dir)

                CircleCIAnalysisCommand._print_complete_results(result)
                logger.info("Complete analysis finished successfully")

        except Exception as e:
            logger.error(f"❌ Analysis failed: {e}")
            exit(1)

    @staticmethod
    def _print_summary(analysis) -> None:
        """Print summary analysis results"""
        print("\\n🎯 CircleCI Performance Summary:")
        print("=" * 50)

        summary = analysis["summary"]
        perf = analysis["pipeline_performance"]

        print("📊 Data Summary:")
        print(f"   Pipelines analyzed: {summary['total_pipelines']}")
        print(f"   Workflows analyzed: {summary['total_workflows']}")
        print(f"   Jobs analyzed: {summary['total_jobs']}")

        print("\\n⏱️  Pipeline Performance:")
        print(f"   Average duration: {perf['avg_duration']}s ({perf['avg_duration'] / 60:.1f}m)")
        print(f"   Success rate: {perf['success_rate']}%")
        print(f"   Failure rate: {perf['failure_rate']}%")

        print("\\n🐌 Slowest Jobs:")
        for i, job in enumerate(analysis["slowest_jobs"][:3], 1):
            print(f"   {i}. {job['name']}: {job['duration_minutes']}m")

    @staticmethod
    def _list_projects(service: "CircleCIService") -> None:
        """List all available CircleCI projects"""
        logger = LogManager.get_instance().get_logger("CircleCIAnalysisCommand")
        logger.info("Starting to list CircleCI projects...")

        try:
            projects = service.list_projects()

            if not projects:
                logger.warning("No projects found")
                print("❌ No projects found or no access to projects")
                print("   Check your CircleCI token permissions")
                return

            logger.info(f"Found {len(projects)} projects, displaying them...")

            print(f"\n🔍 Found {len(projects)} CircleCI projects:")
            print("=" * 100)

            for project in projects:
                name = project.get("name", "Unknown")
                slug = project.get("slug", "Unknown")
                organization = project.get("organization", "Unknown")
                vcs_type = project.get("vcs_type", "Unknown")
                vcs_url = project.get("url", "Unknown")
                default_branch = project.get("default_branch", "Unknown")

                print(f"\n📦 {name}")
                print(f"   Slug: {slug}")
                print(f"   Organization: {organization}")
                print(f"   VCS: {vcs_type}")
                print(f"   Default branch: {default_branch}")
                if vcs_url and vcs_url != "Unknown":
                    print(f"   Repository: {vcs_url}")

            print("\n💡 To analyze a project, use:")
            print("   python src/main.py circleci circleci-analyze --project-slug SLUG")
            print("\n   Example with your first project:")
            if projects:
                first_project = projects[0]
                example_slug = first_project.get("slug", "unknown")
                print(f"   python src/main.py circleci circleci-analyze --project-slug {example_slug}")

        except Exception as e:
            logger.error(f"Error listing projects: {e}")
            print(f"❌ Error listing projects: {e}")
            exit(1)

    @staticmethod
    def _print_complete_results(result) -> None:
        """Print complete analysis results"""
        print("\\n🎯 CircleCI Performance Analysis Complete!")
        print("=" * 60)

        summary = result["summary"]
        bottlenecks = result["bottlenecks"]
        plan = result["optimization_plan"]

        print("📊 Analysis Summary:")
        print(f"   Pipelines: {summary['total_pipelines']}")
        print(f"   Workflows: {summary['total_workflows']}")
        print(f"   Jobs: {summary['total_jobs']}")

        print("\\n🐌 Top Performance Bottlenecks:")
        for i, bottleneck in enumerate(bottlenecks, 1):
            print(
                f"   {i}. {bottleneck['job']}: {bottleneck['avg_duration_minutes']}m "
                f"(Success: {bottleneck['success_rate']}%)"
            )

        print("\\n🚀 Optimization Plan:")
        current_time = plan.get("estimated_final_time", 0) + plan.get("estimated_savings", 0)
        print(f"   Current avg pipeline time: {current_time}s ({current_time / 60:.1f}m)")
        print(f"   Estimated savings: {plan['estimated_savings']}s")
        print(f"   Target pipeline time: {plan['estimated_final_time']}s ({plan['estimated_final_time'] / 60:.1f}m)")

        if plan["immediate_actions"]:
            print("\\n⚡ Immediate Actions:")
            for action in plan["immediate_actions"]:
                print(f"   • {action['action']} ({action['job']})")
                print(f"     Current: {action['current_time']}, Savings: {action['estimated_savings']}")

        if plan["medium_term"]:
            print("\\n🔄 Medium-term Actions:")
            for action in plan["medium_term"]:
                print(f"   • {action['action']} ({action['job']})")
                print(f"     Current: {action['current_time']}, Savings: {action['estimated_savings']}")

        print(f"\\n📁 Results saved to: {result['output_dir']}/")
        print("   Files created:")
        print("   - pipelines.json (pipeline data)")
        print("   - workflows.json (workflow data)")
        print("   - jobs.json (job data)")
        print("   - analysis.json (performance analysis)")
        print("   - recommendations.json (optimization recommendations)")
        print("   - detailed_analysis.json (bottlenecks & optimization plan)")
        print("   - performance_charts.png (visualizations)")

        print("\\n💡 Next Steps:")
        print("   1. Review detailed_analysis.json for specific recommendations")
        print("   2. Implement immediate actions first for quick wins")
        print("   3. Plan medium-term optimizations for sustained improvement")
        print("   4. Re-run analysis after changes to measure impact")
