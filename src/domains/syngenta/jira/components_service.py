from datetime import datetime

from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.logging.logging_manager import LogManager
from utils.output_manager import OutputManager


class ComponentService:
    """Service class for managing Jira components."""

    def __init__(self):
        """Initialize the service with JIRA assistant."""
        self.jira_assistant = JiraAssistant()
        self._logger = LogManager.get_instance().get_logger("ComponentService")

    def list_components(self, project_key: str, output_file: str | None = None) -> list[dict]:
        """List all components in a project."""
        if not project_key:
            raise ValueError("project_key is required for list operation")

        components = self.jira_assistant.fetch_project_components(project_key)

        if not components:
            print(f"No components found in project '{project_key}'.")
            return []

        print(f"\nComponents in project '{project_key}':")
        print("=" * 60)
        for i, component in enumerate(components, 1):
            print(f"{i}. {component.get('name')}")
            print(f"   ID: {component.get('id')}")
            print(f"   Description: {component.get('description', 'No description')}")
            print(f"   Assignee Type: {component.get('assigneeType', 'N/A')}")
            if component.get("lead"):
                print(f"   Lead: {component.get('lead', {}).get('displayName', 'N/A')}")
            print()

        if output_file:
            JSONManager.write_json(components, output_file)
            print(f"Components saved to '{output_file}'.")
        else:
            # Generate default output file in organized structure
            output_path = OutputManager.get_output_path("list-components", f"components_{project_key}")
            JSONManager.write_json(components, output_path)
            print(f"Components saved to '{output_path}'.")

        self._logger.info(f"Listed {len(components)} components for project '{project_key}'.")
        return components

    def create_component(
        self,
        project_key: str,
        name: str,
        description: str | None = None,
        assignee_type: str = "PROJECT_DEFAULT",
        lead: str | None = None,
        output_file: str | None = None,
    ) -> dict:
        """Create a single component."""
        if not project_key:
            raise ValueError("project_key is required for create operation")
        if not name:
            raise ValueError("name is required for create operation")

        component = self.jira_assistant.create_component(
            project_key=project_key,
            name=name,
            description=description if description is not None else "",
            assignee_type=assignee_type,
            lead=lead if lead is not None else "",
        )

        if component:
            print("\nComponent created successfully!")
            print("=" * 40)
            print(f"Name: {component.get('name')}")
            print(f"ID: {component.get('id')}")
            print(f"Project: {component.get('project')}")
            print(f"Description: {component.get('description', 'No description')}")
            print(f"Assignee Type: {component.get('assigneeType')}")
            if component.get("lead"):
                print(f"Lead: {component.get('lead', {}).get('displayName', 'N/A')}")

            if output_file:
                JSONManager.write_json(component, output_file)
                print(f"Component details saved to '{output_file}'.")
            else:
                # Generate default output file in organized structure
                output_path = OutputManager.get_output_path("create-component", f"component_{name.replace(' ', '_')}")
                JSONManager.write_json(component, output_path)
                print(f"Component details saved to '{output_path}'.")

            self._logger.info(f"Component '{name}' created successfully in project '{project_key}'.")
            return component
        else:
            raise RuntimeError(f"Failed to create component '{name}' in project '{project_key}'.")

    def delete_component(self, component_id: str) -> bool:
        """Delete a single component."""
        if not component_id:
            raise ValueError("component_id is required for delete operation")

        success = self.jira_assistant.delete_component(component_id)

        if success:
            print(f"\nComponent '{component_id}' deleted successfully!")
            self._logger.info(f"Component '{component_id}' deleted successfully.")
            return True
        else:
            raise RuntimeError(f"Failed to delete component '{component_id}'.")

    def create_components_batch(self, project_key: str, input_file: str, output_file: str | None = None) -> list[dict]:
        """Create multiple components from a JSON file."""
        if not project_key:
            raise ValueError("project_key is required for create-batch operation")
        if not input_file:
            raise ValueError("input_file is required for create-batch operation")

        try:
            components_data = JSONManager.read_json(input_file)
        except Exception as e:
            raise ValueError(f"Failed to read input file '{input_file}': {e}")

        if not isinstance(components_data, list):
            raise ValueError("Input file must contain a JSON array of component objects")

        results = self.jira_assistant.create_components_batch(project_key, components_data)

        print(f"\nBatch component creation results for project '{project_key}':")
        print("=" * 70)

        success_count = 0
        error_count = 0

        for result in results:
            status = result.get("status")
            name = result.get("name")

            if status == "success":
                success_count += 1
                component = result.get("component", {})
                print(f"✓ Created: {name} (ID: {component.get('id')})")
            else:
                error_count += 1
                error = result.get("error", "Unknown error")
                print(f"✗ Failed: {name} - {error}")

        print(f"\nSummary: {success_count} created, {error_count} failed")

        if output_file:
            JSONManager.write_json(results, output_file)
            print(f"Detailed results saved to '{output_file}'.")
        else:
            # Generate default output file in organized structure
            output_path = OutputManager.get_output_path("create-components-batch", f"batch_creation_{project_key}")
            JSONManager.write_json(results, output_path)
            print(f"Detailed results saved to '{output_path}'.")

        self._logger.info(f"Batch creation completed: {success_count} success, {error_count} errors.")
        return results

    def delete_components_batch(self, component_ids: list[str], output_file: str | None = None) -> list[dict]:
        """Delete multiple components by IDs."""
        if not component_ids:
            raise ValueError("component_ids is required for delete-batch operation")

        if not component_ids:
            raise ValueError("No valid component IDs provided")

        results = self.jira_assistant.delete_components_batch(component_ids)

        print("\nBatch component deletion results:")
        print("=" * 50)

        success_count = 0
        error_count = 0

        for result in results:
            status = result.get("status")
            component_id = result.get("component_id")

            if status == "success":
                success_count += 1
                print(f"✓ Deleted: {component_id}")
            else:
                error_count += 1
                error = result.get("error", "Unknown error")
                print(f"✗ Failed: {component_id} - {error}")

        print(f"\nSummary: {success_count} deleted, {error_count} failed")

        if output_file:
            JSONManager.write_json(results, output_file)
            print(f"Detailed results saved to '{output_file}'.")
        else:
            # Generate default output file in organized structure
            output_path = OutputManager.get_output_path("delete-components-batch", "batch_deletion")
            JSONManager.write_json(results, output_path)
            print(f"Detailed results saved to '{output_path}'.")

        self._logger.info(f"Batch deletion completed: {success_count} success, {error_count} errors.")
        return results

    def update_issue_components(self, issue_key: str, component_id: str, output_file: str | None = None) -> bool:
        """Update an issue to replace all existing components with a single new component.

        Args:
            issue_key (str): The key of the issue to update.
            component_id (str): The ID of the component to set on the issue.
            output_file (str): Optional file path to save the result.

        Returns:
            bool: True if successful, False otherwise.
        """
        if not issue_key:
            raise ValueError("issue_key is required for update operation")
        if not component_id:
            raise ValueError("component_id is required for update operation")

        try:
            success = self.jira_assistant.update_issue_components(issue_key, component_id)

            result = {
                "issue_key": issue_key,
                "component_id": component_id,
                "status": "success" if success else "failed",
                "timestamp": datetime.now().isoformat(),
            }

            if success:
                print(f"✓ Successfully updated issue '{issue_key}' with component ID '{component_id}'")
            else:
                print(f"✗ Failed to update issue '{issue_key}'")

            if output_file:
                JSONManager.write_json(result, output_file)
                print(f"Result saved to '{output_file}'.")
            else:
                # Generate default output file in organized structure
                output_path = OutputManager.get_output_path("update-issue-components", f"update_{issue_key}")
                JSONManager.write_json(result, output_path)
                print(f"Result saved to '{output_path}'.")

            self._logger.info(f"Updated issue '{issue_key}' with component '{component_id}': {success}")
            return success

        except Exception as e:
            error_msg = f"Failed to update issue '{issue_key}': {e}"
            print(f"✗ {error_msg}")
            self._logger.error(error_msg)

            if output_file:
                result = {
                    "issue_key": issue_key,
                    "component_id": component_id,
                    "status": "error",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
                JSONManager.write_json(result, output_file)
                print(f"Error details saved to '{output_file}'.")

            return False

    def update_issues_components_batch(self, input_file: str, output_file: str | None = None) -> list[dict]:
        """Update multiple issues with their respective components from a JSON file.

        Args:
            input_file (str): Path to JSON file containing issue-component mappings.
            output_file (str): Optional file path to save the results.

        Returns:
            List[Dict]: List of update results.
        """
        if not input_file:
            raise ValueError("input_file is required for batch update operation")

        try:
            issues_components = JSONManager.read_json(input_file)
        except Exception as e:
            error_msg = f"Failed to read input file '{input_file}': {e}"
            print(f"✗ {error_msg}")
            self._logger.error(error_msg)
            return []

        if not issues_components or len(issues_components) == 0:
            print("No issue-component mappings found in the input file.")
            return []

        print(f"Updating components for {len(issues_components)} issues...")
        print("=" * 60)

        results = self.jira_assistant.update_issues_components_batch(issues_components)

        # Count and display results
        success_count = sum(1 for r in results if r.get("status") == "success")
        error_count = len(results) - success_count

        # Display individual results
        for result in results:
            issue_key = result.get("issue_key")
            component_id = result.get("component_id")
            status = result.get("status")

            if status == "success":
                print(f"✓ {issue_key} → Component {component_id}")
            else:
                error = result.get("error", "Unknown error")
                print(f"✗ Failed: {issue_key} - {error}")

        print(f"\nSummary: {success_count} updated, {error_count} failed")

        if output_file:
            # Add timestamp to results
            timestamped_results = {
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total": len(results),
                    "successful": success_count,
                    "failed": error_count,
                },
                "results": results,
            }
            JSONManager.write_json(timestamped_results, output_file)
            print(f"Detailed results saved to '{output_file}'.")
        else:
            # Generate default output file in organized structure
            timestamped_results = {
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total": len(results),
                    "successful": success_count,
                    "failed": error_count,
                },
                "results": results,
            }
            output_path = OutputManager.get_output_path("update-issues-components-batch", "batch_update")
            JSONManager.write_json(timestamped_results, output_path)
            print(f"Detailed results saved to '{output_path}'.")

        self._logger.info(f"Batch component update completed: {success_count} success, {error_count} errors.")
        return results
