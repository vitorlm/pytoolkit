"""
JIRA Components Management Command

This unified command provides comprehensive JIRA component management functionality:

OPERATIONS:
- list: List all components in a project
- create: Create a single component
- delete: Delete a single component
- create-batch: Create multiple components from JSON file
- delete-batch: Delete multiple components by IDs
- update-issue: Update an issue's components (replace all with single component)
- update-issues-batch: Update multiple issues' components from JSON file

USAGE EXAMPLES:

1. List components:
   python src/main.py syngenta jira components --operation list --project-key "PROJ"

2. Create single component:
   python src/main.py syngenta jira components --operation create --project-key "PROJ"
   --name "Frontend" --description "UI components" --assignee-type "PROJECT_DEFAULT"

3. Delete component:
   python src/main.py syngenta jira components --operation delete --component-id "10001"

4. Batch create from JSON:
   python src/main.py syngenta jira components --operation create-batch --project-key "PROJ"
   --input-file "example_components.json"

5. Batch delete:
   python src/main.py syngenta jira components --operation delete-batch
   --component-ids "10001,10002,10003"

6. Update single issue components:
   python src/main.py syngenta jira components --operation update-issue
   --issue-key "CWS-123" --component-id "15296"

7. Batch update issue components:
   python src/main.py syngenta jira components --operation update-issues-batch
   --input-file "classified_issues.json"

JSON FORMAT (for batch create):
[
  {
    "name": "Component Name",
    "description": "Optional description",
    "assignee_type": "PROJECT_DEFAULT|COMPONENT_LEAD|PROJECT_LEAD|UNASSIGNED",
    "lead": "optional-account-id"
  }
]

JSON FORMAT (for batch issue update):
[
  {
    "key": "CWS-123",
    "component": 15296
  },
  {
    "key": "CWS-124",
    "component": 15297
  }
]

ASSIGNEE TYPES:
- PROJECT_DEFAULT: Use project's default assignee
- COMPONENT_LEAD: Assign to component lead
- PROJECT_LEAD: Assign to project lead
- UNASSIGNED: No automatic assignment
"""

from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager
from domains.syngenta.jira.components_service import ComponentService


class ComponentsCommand(BaseCommand):
    """Unified command to manage JIRA project components."""

    @staticmethod
    def get_name() -> str:
        return "components"

    @staticmethod
    def get_description() -> str:
        return (
            "Manage JIRA project components (list, create, delete, batch operations)."
        )

    @staticmethod
    def get_help() -> str:
        return "This command provides comprehensive component management for JIRA projects."

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--operation",
            type=str,
            required=True,
            choices=[
                "list",
                "create",
                "delete",
                "create-batch",
                "delete-batch",
                "update-issue",
                "update-issues-batch",
            ],
            help="The operation to perform on components.",
        )
        parser.add_argument(
            "--project-key",
            type=str,
            required=False,
            help="The JIRA project key (required for list, create, create-batch operations).",
        )
        parser.add_argument(
            "--name",
            type=str,
            required=False,
            help="The name of the component (required for create operation).",
        )
        parser.add_argument(
            "--description",
            type=str,
            required=False,
            help="Optional description for the component (create operation).",
        )
        parser.add_argument(
            "--assignee-type",
            type=str,
            required=False,
            default="PROJECT_DEFAULT",
            choices=["PROJECT_DEFAULT", "COMPONENT_LEAD", "PROJECT_LEAD", "UNASSIGNED"],
            help="The type of assignee for the component "
            "(create operation, default: PROJECT_DEFAULT).",
        )
        parser.add_argument(
            "--lead",
            type=str,
            required=False,
            help="Optional account ID of the component lead (create operation).",
        )
        parser.add_argument(
            "--component-id",
            type=str,
            required=False,
            help="The component ID (required for delete operation).",
        )
        parser.add_argument(
            "--component-ids",
            type=str,
            required=False,
            help="Comma-separated component IDs (required for delete-batch operation).",
        )
        parser.add_argument(
            "--input-file",
            type=str,
            required=False,
            help="JSON file containing component definitions (create-batch) or "
            "issue-component mappings (update-issues-batch).",
        )
        parser.add_argument(
            "--issue-key",
            type=str,
            required=False,
            help="The issue key to update (required for update-issue operation).",
        )
        parser.add_argument(
            "--output-file",
            type=str,
            required=False,
            help="Optional file path to save the results in JSON format.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to execute component operations.

        Args:
            args (Namespace): Command-line arguments.
        """
        # Ensure environment variables are loaded
        ensure_env_loaded()

        operation = args.operation
        _logger = LogManager.get_instance().get_logger("ComponentsCommand")

        try:
            service = ComponentService()

            if operation == "list":
                ComponentsCommand._list_components(args, service)
            elif operation == "create":
                ComponentsCommand._create_component(args, service)
            elif operation == "delete":
                ComponentsCommand._delete_component(args, service)
            elif operation == "create-batch":
                ComponentsCommand._create_components_batch(args, service)
            elif operation == "delete-batch":
                ComponentsCommand._delete_components_batch(args, service)
            elif operation == "update-issue":
                ComponentsCommand._update_issue_components(args, service)
            elif operation == "update-issues-batch":
                ComponentsCommand._update_issues_components_batch(args, service)
        except Exception as e:
            _logger.error(f"Failed to execute {operation} operation: {e}")
            print(f"Error: Failed to execute {operation} operation: {e}")
            exit(1)

    @staticmethod
    def _list_components(args: Namespace, service: ComponentService):
        """List all components in a project."""
        service.list_components(
            project_key=args.project_key, output_file=args.output_file
        )

    @staticmethod
    def _create_component(args: Namespace, service: ComponentService):
        """Create a single component."""
        service.create_component(
            project_key=args.project_key,
            name=args.name,
            description=args.description,
            assignee_type=args.assignee_type,
            lead=args.lead,
            output_file=args.output_file,
        )

    @staticmethod
    def _delete_component(args: Namespace, service: ComponentService):
        """Delete a single component."""
        service.delete_component(component_id=args.component_id)

    @staticmethod
    def _create_components_batch(args: Namespace, service: ComponentService):
        """Create multiple components from a JSON file."""
        service.create_components_batch(
            project_key=args.project_key,
            input_file=args.input_file,
            output_file=args.output_file,
        )

    @staticmethod
    def _delete_components_batch(args: Namespace, service: ComponentService):
        """Delete multiple components by IDs."""
        component_ids = [id.strip() for id in args.component_ids.split(",")]
        service.delete_components_batch(
            component_ids=component_ids, output_file=args.output_file
        )

    @staticmethod
    def _update_issue_components(args: Namespace, service: ComponentService):
        """Update a single issue's components."""
        service.update_issue_components(
            issue_key=args.issue_key,
            component_id=args.component_id,
            output_file=args.output_file,
        )

    @staticmethod
    def _update_issues_components_batch(args: Namespace, service: ComponentService):
        """Update multiple issues' components from a JSON file."""
        service.update_issues_components_batch(
            input_file=args.input_file, output_file=args.output_file
        )
