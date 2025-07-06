"""
SonarQube Data Command

This command provides access to SonarQube API data for code quality analysis with
caching support for improved performance.

OPERATIONS:
- projects: List all projects in SonarQube (supports predefined project list)
- measures: Get project measures/metrics for a single project
- issues: Get project issues with severity filtering
- metrics: List all available metrics
- batch-measures: Get measures for multiple projects in one request
- list-projects: Get projects from predefined list (projects_list.json) with optional measures

PREDEFINED PROJECT LIST:
The command uses a predefined list of 27 projects stored in:
src/domains/syngenta/sonarqube/projects_list.json

This includes core Syngenta Digital projects like:
- API services (Java, Node.js, Python)
- Web applications (React)
- Backend services and integrations

USAGE EXAMPLES:

1. List all projects from SonarQube:
   python src/main.py syngenta sonarqube --operation projects

2. List projects from predefined list:
   python src/main.py syngenta sonarqube --operation projects --use-project-list

3. Get quality metrics for predefined projects (RECOMMENDED):
   python src/main.py syngenta sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures

4. Get quality metrics for predefined projects and save to file:
   python src/main.py syngenta sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures \
       --output-file "syngenta_projects_metrics.json"

5. Get custom metrics for predefined projects:
   python src/main.py syngenta sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures \
       --metrics "bugs,vulnerabilities,coverage,duplicated_lines_density"

6. Get project measures for a specific project:
   python src/main.py syngenta sonarqube --operation measures \
       --project-key "syngenta_digital_api_java_server_strix" \
       --metrics "lines,bugs,vulnerabilities,code_smells"

7. Get batch measures for multiple specific projects:
   python src/main.py syngenta sonarqube --operation batch-measures \
       --project-keys "project1,project2,project3" \
       --metrics "bugs,vulnerabilities,coverage"

8. Get project issues with severity filtering:
   python src/main.py syngenta sonarqube --operation issues \
       --project-key "syngenta_digital_api_java_server_strix" \
       --severities "MAJOR,CRITICAL,BLOCKER"

9. List all available metrics:
   python src/main.py syngenta sonarqube --operation metrics

CACHING:
- Results are cached for 1 hour to improve performance
- Cache is automatically managed and transparent to users
- Subsequent requests for the same data will be faster
- Use --clear-cache to force refresh and clear existing cache

CACHE MANAGEMENT:
10. Clear cache and get fresh data:
   python src/main.py syngenta sonarqube sonarqube --operation list-projects \
       --organization "syngenta-digital" --include-measures --clear-cache

ENVIRONMENT VARIABLES:
- SONARQUBE_URL: SonarQube instance URL (default: https://sonarcloud.io)
- SONARQUBE_TOKEN: Authentication token for API access
- SONARQUBE_ORGANIZATION: Organization key (default: syngenta-digital)

DEFAULT QUALITY METRICS (used when --include-measures without --metrics):
- alert_status: Overall quality gate status
- bugs: Number of bugs
- reliability_rating: Reliability rating (A-E)
- vulnerabilities: Number of vulnerabilities
- security_rating: Security rating (A-E)
- security_hotspots_reviewed: Security hotspots review percentage
- security_review_rating: Security review rating (A-E)
- code_smells: Number of code smells
- sqale_rating: Maintainability rating (A-E)
- duplicated_lines_density: Duplicated lines percentage
- coverage: Test coverage percentage
- ncloc: Non-commented lines of code
- ncloc_language_distribution: Language distribution
- security_issues: Number of security issues
- reliability_issues: Number of reliability issues
- maintainability_issues: Number of maintainability issues

COMMON METRICS FOR CUSTOM ANALYSIS:
- lines: Total lines of code
- complexity: Cyclomatic complexity
- cognitive_complexity: Cognitive complexity
- test_success_density: Test success density
- branch_coverage: Branch coverage percentage
- line_coverage: Line coverage percentage
- security_remediation_effort: Security remediation effort
- reliability_remediation_effort: Reliability remediation effort
- sqale_index: Technical debt ratio
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from domains.syngenta.sonarqube.sonarqube_service import SonarQubeService


class SonarQubeCommand(BaseCommand):
    """Command to fetch data from SonarQube API."""

    @staticmethod
    def get_name() -> str:
        return "sonarqube"

    @staticmethod
    def get_description() -> str:
        return "Fetch code quality data from SonarQube API."

    @staticmethod
    def get_help() -> str:
        return "Access SonarQube API to retrieve project metrics, issues, and quality gate data."

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--operation",
            type=str,
            required=True,
            choices=[
                "projects",
                "measures",
                "issues",
                "metrics",
                "batch-measures",
                "list-projects",
            ],
            help=(
                "The operation to perform: projects, measures, issues, metrics, "
                "batch-measures, or list-projects."
            ),
        )
        parser.add_argument(
            "--project-key",
            type=str,
            required=False,
            help="The project key (required for measures and issues operations).",
        )
        parser.add_argument(
            "--project-keys",
            type=str,
            required=False,
            help="Comma-separated list of project keys (for batch-measures operation).",
        )
        parser.add_argument(
            "--organization",
            type=str,
            required=False,
            help="Organization key (for SonarCloud).",
        )
        parser.add_argument(
            "--use-project-list",
            action="store_true",
            help="Use predefined project list instead of fetching all projects.",
        )
        parser.add_argument(
            "--include-measures",
            action="store_true",
            help="Include measures when fetching projects from list.",
        )
        parser.add_argument(
            "--metrics",
            type=str,
            required=False,
            default=(
                "alert_status,bugs,reliability_rating,vulnerabilities,security_rating,"
                "security_hotspots_reviewed,security_review_rating,code_smells,sqale_rating,"
                "duplicated_lines_density,coverage,ncloc,ncloc_language_distribution,"
                "security_issues,reliability_issues,maintainability_issues"
            ),
            help="Comma-separated list of metrics to fetch (for measures operation).",
        )
        parser.add_argument(
            "--severities",
            type=str,
            required=False,
            help="Comma-separated list of issue severities to filter by "
            + "(INFO,MINOR,MAJOR,CRITICAL,BLOCKER).",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the results in JSON format.",
        )
        parser.add_argument(
            "--base-url",
            type=str,
            required=False,
            help="SonarQube instance URL (overrides SONARQUBE_URL environment variable).",
        )
        parser.add_argument(
            "--token",
            type=str,
            required=False,
            help="Authentication token (overrides SONARQUBE_TOKEN environment variable).",
        )
        parser.add_argument(
            "--clear-cache",
            action="store_true",
            help="Clear the cache before executing the operation.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute SonarQube operations.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        operation = args.operation
        _logger = LogManager.get_instance().get_logger("SonarQubeCommand")

        try:
            service = SonarQubeService(base_url=args.base_url, token=args.token)

            # Clear cache if requested
            if args.clear_cache:
                _logger.info("Clearing cache before operation")
                service.clear_cache()

            if operation == "projects":
                SonarQubeCommand._list_projects(args, service)
            elif operation == "measures":
                SonarQubeCommand._get_project_measures(args, service)
            elif operation == "issues":
                SonarQubeCommand._get_project_issues(args, service)
            elif operation == "metrics":
                SonarQubeCommand._list_available_metrics(args, service)
            elif operation == "batch-measures":
                SonarQubeCommand._get_batch_measures(args, service)
            elif operation == "list-projects":
                SonarQubeCommand._get_projects_from_list(args, service)
        except Exception as e:
            _logger.error(f"Failed to execute {operation} operation: {e}")
            exit(1)

    @staticmethod
    def _list_projects(args: Namespace, service: SonarQubeService):
        """List all projects."""
        service.get_projects(
            organization=args.organization,
            use_project_list=args.use_project_list,
            output_file=args.output_file,
        )

    @staticmethod
    def _get_project_measures(args: Namespace, service: SonarQubeService):
        """Get project measures."""
        if not args.project_key:
            logger = LogManager.get_instance().get_logger("SonarQubeCommand")
            logger.error("--project-key is required for measures operation")
            exit(1)

        metrics = [metric.strip() for metric in args.metrics.split(",")]
        service.get_project_measures(
            project_key=args.project_key, metric_keys=metrics, output_file=args.output_file
        )

    @staticmethod
    def _get_project_issues(args: Namespace, service: SonarQubeService):
        """Get project issues."""
        if not args.project_key:
            logger = LogManager.get_instance().get_logger("SonarQubeCommand")
            logger.error("--project-key is required for issues operation")
            exit(1)

        severities = None
        if args.severities:
            severities = [severity.strip() for severity in args.severities.split(",")]

        service.get_project_issues(
            project_key=args.project_key, severities=severities, output_file=args.output_file
        )

    @staticmethod
    def _list_available_metrics(args: Namespace, service: SonarQubeService):
        """List all available metrics."""
        service.get_available_metrics(output_file=args.output_file)

    @staticmethod
    def _get_batch_measures(args: Namespace, service: SonarQubeService):
        """Get measures for multiple projects."""
        if not args.project_keys:
            logger = LogManager.get_instance().get_logger("SonarQubeCommand")
            logger.error("--project-keys is required for batch-measures operation")
            exit(1)

        project_keys = [key.strip() for key in args.project_keys.split(",")]
        metrics = [metric.strip() for metric in args.metrics.split(",")]

        service.get_batch_measures(
            project_keys=project_keys, metric_keys=metrics, output_file=args.output_file
        )

    @staticmethod
    def _get_projects_from_list(args: Namespace, service: SonarQubeService):
        """Get projects from predefined list with optional measures."""
        metric_keys = None

        if args.include_measures:
            if args.metrics:
                metric_keys = [metric.strip() for metric in args.metrics.split(",")]
            else:
                # Default metrics for quality analysis
                metric_keys = [
                    "alert_status",
                    "bugs",
                    "reliability_rating",
                    "vulnerabilities",
                    "security_rating",
                    "security_hotspots_reviewed",
                    "security_review_rating",
                    "code_smells",
                    "sqale_rating",
                    "duplicated_lines_density",
                    "coverage",
                    "ncloc",
                    "ncloc_language_distribution",
                    "security_issues",
                    "reliability_issues",
                    "maintainability_issues",
                ]

        service.get_projects_by_list(
            organization=args.organization,
            include_measures=args.include_measures,
            metric_keys=metric_keys,
            output_file=args.output_file,
        )
