"""
JIRA Workflow Configuration Management Command

This command provides utilities to manage, validate, and test workflow configurations.

FUNCTIONALITY:
- Validate workflow configurations for consistency
- List available workflow configurations
- Test workflow configuration for specific projects
- Clear workflow configuration cache

USAGE EXAMPLES:

1. Validate workflow configuration for CWS project:
   python src/main.py syngenta jira workflow-config --project-key "CWS" --operation validate

2. List all available workflow configurations:
   python src/main.py syngenta jira workflow-config --operation list

3. Test workflow configuration and show status mappings:
   python src/main.py syngenta jira workflow-config --project-key "CWS" --operation test

4. Clear workflow configuration cache:
   python src/main.py syngenta jira workflow-config --operation clear-cache

5. Show detailed workflow information:
   python src/main.py syngenta jira workflow-config --project-key "CWS" --operation show --verbose
"""

from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.workflow_config_service import WorkflowConfigService
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class WorkflowConfigCommand(BaseCommand):
    """Command to manage and validate workflow configurations."""

    @staticmethod
    def get_name() -> str:
        return "workflow-config"

    @staticmethod
    def get_description() -> str:
        return "Manage and validate JIRA workflow configurations for different projects."

    @staticmethod
    def get_help() -> str:
        return (
            "This command provides utilities to manage, validate, and test workflow "
            "configurations for different JIRA projects, enabling semantic status mapping."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--project-key",
            type=str,
            required=False,
            help="The JIRA project key to validate/test (e.g., 'CWS', 'PROJ').",
        )
        parser.add_argument(
            "--operation",
            type=str,
            required=True,
            choices=["validate", "list", "test", "show", "clear-cache"],
            help=(
                "Operation to perform: "
                "'validate' - validate workflow config consistency, "
                "'list' - list available project configurations, "
                "'test' - test workflow config functionality, "
                "'show' - show detailed workflow information, "
                "'clear-cache' - clear workflow configuration cache."
            ),
        )
        parser.add_argument(
            "--verbose",
            action="store_true",
            help="Enable verbose output with detailed information.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute workflow configuration operations.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        logger = LogManager.get_instance().get_logger("WorkflowConfigCommand")

        try:
            # Initialize workflow config service
            workflow_service = WorkflowConfigService()

            if args.operation == "list":
                WorkflowConfigCommand._list_configurations(workflow_service, args.verbose)

            elif args.operation == "validate":
                if not args.project_key:
                    logger.error("--project-key is required for validate operation")
                    exit(1)
                WorkflowConfigCommand._validate_configuration(workflow_service, args.project_key, args.verbose)

            elif args.operation == "test":
                if not args.project_key:
                    logger.error("--project-key is required for test operation")
                    exit(1)
                WorkflowConfigCommand._test_configuration(workflow_service, args.project_key, args.verbose)

            elif args.operation == "show":
                if not args.project_key:
                    logger.error("--project-key is required for show operation")
                    exit(1)
                WorkflowConfigCommand._show_configuration(workflow_service, args.project_key, args.verbose)

            elif args.operation == "clear-cache":
                WorkflowConfigCommand._clear_cache(workflow_service, args.project_key)

            logger.info("Workflow configuration operation completed successfully")

        except Exception as e:
            logger.error(f"Failed to execute workflow configuration operation: {e}")
            exit(1)

    @staticmethod
    def _list_configurations(workflow_service: WorkflowConfigService, verbose: bool):
        """List available workflow configurations."""
        print("\n" + "=" * 60)
        print("AVAILABLE WORKFLOW CONFIGURATIONS")
        print("=" * 60)

        available_projects = workflow_service.get_available_projects()

        if not available_projects:
            print("No project configurations found.")
            return

        for project_key in available_projects:
            try:
                config = workflow_service.get_workflow_config(project_key)
                print(f"\nProject: {project_key}")
                print(f"  Name: {config.get('project_name', 'N/A')}")
                print(f"  Workflow: {config.get('workflow_name', 'N/A')}")
                print(f"  Version: {config.get('version', 'N/A')}")

                if verbose:
                    status_mapping = config.get("status_mapping", {})
                    print("  Status Categories:")
                    for category, statuses in status_mapping.items():
                        print(f"    {category.title()}: {len(statuses)} statuses")

            except Exception as e:
                print(f"  Error loading config: {e}")

        print(f"\nTotal configurations: {len(available_projects)}")

    @staticmethod
    def _validate_configuration(workflow_service: WorkflowConfigService, project_key: str, verbose: bool):
        """Validate workflow configuration for a project."""
        print("\n" + "=" * 60)
        print(f"VALIDATING WORKFLOW CONFIGURATION: {project_key}")
        print("=" * 60)

        validation_results = workflow_service.validate_workflow_config(project_key)

        errors = validation_results.get("errors", [])
        warnings = validation_results.get("warnings", [])

        if errors:
            print(f"\n❌ ERRORS ({len(errors)}):")
            for i, error in enumerate(errors, 1):
                print(f"  {i}. {error}")

        if warnings:
            print(f"\n⚠️  WARNINGS ({len(warnings)}):")
            for i, warning in enumerate(warnings, 1):
                print(f"  {i}. {warning}")

        if not errors and not warnings:
            print("\n✅ VALIDATION PASSED")
            print("No errors or warnings found.")
        elif not errors:
            print("\n✅ VALIDATION PASSED WITH WARNINGS")
            print(f"Configuration is valid but has {len(warnings)} warnings.")
        else:
            print("\n❌ VALIDATION FAILED")
            print(f"Found {len(errors)} errors and {len(warnings)} warnings.")

        if verbose and not errors:
            try:
                config = workflow_service.get_workflow_config(project_key)
                print("\nConfiguration Details:")
                print(f"  Project Name: {config.get('project_name')}")
                print(f"  Workflow Name: {config.get('workflow_name')}")
                print(f"  Total Statuses: {sum(len(statuses) for statuses in config.get('status_mapping', {}).values())}")
                print(f"  Flow Metrics: {len(config.get('flow_metrics', {}))}")
                print(f"  Quality Gates: {len(config.get('quality_gates', {}))}")
            except Exception as e:
                print(f"  Error loading additional details: {e}")

    @staticmethod
    def _test_configuration(workflow_service: WorkflowConfigService, project_key: str, verbose: bool):
        """Test workflow configuration functionality."""
        print("\n" + "=" * 60)
        print(f"TESTING WORKFLOW CONFIGURATION: {project_key}")
        print("=" * 60)

        try:
            # Test basic status categorization
            test_statuses = ["07 Started", "10 Done", "01 New", "08 Testing", "Invalid Status"]

            print("\n🧪 TESTING STATUS CATEGORIZATION:")
            for status in test_statuses:
                category = workflow_service.get_status_category(project_key, status)
                is_wip = workflow_service.is_wip_status(project_key, status)
                is_done = workflow_service.is_done_status(project_key, status)
                is_backlog = workflow_service.is_backlog_status(project_key, status)

                print(f"  '{status}' -> Category: {category or 'UNKNOWN'}, WIP: {is_wip}, Done: {is_done}, Backlog: {is_backlog}")

            # Test semantic status mapping
            print("\n🎯 TESTING SEMANTIC STATUS MAPPING:")
            semantic_tests = ["development_start", "code_review", "testing", "completed", "nonexistent"]
            for semantic in semantic_tests:
                status = workflow_service.get_semantic_status(project_key, semantic)
                print(f"  '{semantic}' -> '{status or 'NOT FOUND'}'")

            # Test flow metrics
            print("\n📊 TESTING FLOW METRICS:")
            try:
                cycle_start, cycle_end = workflow_service.get_cycle_time_statuses(project_key)
                print(f"  Cycle Time: '{cycle_start}' -> '{cycle_end}'")
            except Exception as e:
                print(f"  Cycle Time: ERROR - {e}")

            try:
                lead_start, lead_end = workflow_service.get_lead_time_statuses(project_key)
                print(f"  Lead Time: '{lead_start}' -> '{lead_end}'")
            except Exception as e:
                print(f"  Lead Time: ERROR - {e}")

            # Test custom fields
            print("\n🔧 TESTING CUSTOM FIELDS:")
            custom_field_tests = ["squad_field", "epic_link_field", "story_points_field", "nonexistent_field"]
            for field in custom_field_tests:
                field_id = workflow_service.get_custom_field(project_key, field)
                print(f"  '{field}' -> '{field_id or 'NOT FOUND'}'")

            if verbose:
                print("\n📋 COMPLETE STATUS LISTS:")
                wip_statuses = workflow_service.get_wip_statuses(project_key)
                done_statuses = workflow_service.get_done_statuses(project_key)
                backlog_statuses = workflow_service.get_backlog_statuses(project_key)

                print(f"  WIP Statuses ({len(wip_statuses)}): {wip_statuses}")
                print(f"  Done Statuses ({len(done_statuses)}): {done_statuses}")
                print(f"  Backlog Statuses ({len(backlog_statuses)}): {backlog_statuses}")

            print("\n✅ TESTING COMPLETED")

        except Exception as e:
            print(f"\n❌ TESTING FAILED: {e}")

    @staticmethod
    def _show_configuration(workflow_service: WorkflowConfigService, project_key: str, verbose: bool):
        """Show detailed workflow configuration."""
        print("\n" + "=" * 60)
        print(f"WORKFLOW CONFIGURATION: {project_key}")
        print("=" * 60)

        try:
            config = workflow_service.get_workflow_config(project_key)

            # Basic info
            print("\nProject Information:")
            print(f"  Project Key: {config.get('project_key')}")
            print(f"  Project Name: {config.get('project_name')}")
            print(f"  Workflow Name: {config.get('workflow_name')}")
            print(f"  Version: {config.get('version')}")
            print(f"  Description: {config.get('description', 'N/A')}")

            # Status mapping
            print("\nStatus Mapping:")
            status_mapping = config.get("status_mapping", {})
            for category, statuses in status_mapping.items():
                print(f"  {category.title()} ({len(statuses)}):")
                for status in statuses:
                    print(f"    - {status}")

            # Semantic statuses
            print("\nSemantic Status Mapping:")
            semantic_statuses = config.get("semantic_statuses", {})
            for semantic, status in semantic_statuses.items():
                print(f"  {semantic}: {status}")

            # Flow metrics
            print("\nFlow Metrics:")
            flow_metrics = config.get("flow_metrics", {})
            for metric_name, metric_config in flow_metrics.items():
                print(f"  {metric_name}:")
                print(f"    Start: {metric_config.get('start')}")
                end = metric_config.get('end')
                if isinstance(end, list):
                    print(f"    End: {', '.join(end)}")
                else:
                    print(f"    End: {end}")
                if verbose:
                    print(f"    Description: {metric_config.get('description', 'N/A')}")

            # Quality gates
            quality_gates = config.get("quality_gates", {})
            if quality_gates:
                print("\nQuality Gates:")
                for gate_name, gate_config in quality_gates.items():
                    print(f"  {gate_name}: {gate_config.get('status')}")
                    if verbose:
                        print(f"    Description: {gate_config.get('description', 'N/A')}")

            # Custom fields
            custom_fields = config.get("custom_fields", {})
            if custom_fields:
                print("\nCustom Fields:")
                for field_name, field_id in custom_fields.items():
                    print(f"  {field_name}: {field_id}")

            if verbose:
                # Transitions
                transitions = config.get("transitions", [])
                if transitions:
                    print(f"\nTransitions ({len(transitions)}):")
                    for transition in transitions:
                        print(f"  {transition.get('from')} -> {transition.get('to')} ({transition.get('name')})")

        except Exception as e:
            print(f"\n❌ Failed to show configuration: {e}")

    @staticmethod
    def _clear_cache(workflow_service: WorkflowConfigService, project_key: str = None):
        """Clear workflow configuration cache."""
        print("\n" + "=" * 60)
        if project_key:
            print(f"CLEARING CACHE FOR: {project_key}")
        else:
            print("CLEARING ALL WORKFLOW CACHE")
        print("=" * 60)

        try:
            workflow_service.clear_cache(project_key)
            if project_key:
                print(f"✅ Cache cleared for project: {project_key}")
            else:
                print("✅ All workflow configuration cache cleared")
        except Exception as e:
            print(f"❌ Failed to clear cache: {e}")