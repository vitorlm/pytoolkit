import os
from argparse import Namespace
from dataclasses import dataclass
from typing import Any

from utils.data.json_manager import JSONManager
from utils.jira.jira_assistant import JiraAssistant
from utils.llm.llm_client import LLMMessage, LLMRequest
from utils.llm.llm_factory import LLMFactory
from utils.logging.logging_manager import LogManager


@dataclass
class ClassificationResult:
    """Result of classifying a single issue."""

    issue_key: str
    issue_title: str
    original_component: str
    predicted_component: str
    jira_component_id: str = ""
    confidence: float = 1.0  # Future: extract from LLM if supported


class ComponentClassifierService:
    """Service to classify JIRA issues into components using LLM."""

    def __init__(self, llm_provider: str, llm_model: str):
        """Initialize classifier service.

        Args:
            llm_provider: LLM provider name ('portkey', 'zai', etc.)
            llm_model: LLM model name (e.g., 'glm-4.7', 'openai/gpt-4o-mini')
        """
        self.logger = LogManager.get_instance().get_logger("ComponentClassifierService")
        self.jira_assistant = JiraAssistant(cache_expiration=60)
        self.llm_client = LLMFactory.create_client(llm_provider)
        self.llm_provider = llm_provider
        self.llm_model = llm_model
        self.config = self._load_config()

        self.logger.info(f"Initialized with LLM provider: {self.llm_provider}, model: {self.llm_model}")

    def _load_config(self) -> dict[str, Any]:
        """Load classifier configuration from JSON."""
        config_path = os.path.join(os.path.dirname(__file__), "component_classifier_config.json")
        return JSONManager.read_json(config_path)

    def execute(self, args: Namespace) -> dict[str, Any]:
        """Execute component classification.

        Args:
            args: Command arguments with source_component, project_key, etc.

        Returns:
            Dictionary with classification results and statistics
        """
        source_component = args.source_component
        project_key = args.project_key
        dry_run = getattr(args, "dry_run", False)

        self.logger.info(f"Starting classification for component: {source_component}")

        # Fetch issues with source component
        issues = self._fetch_issues_by_component(project_key, source_component)
        self.logger.info(f"Found {len(issues)} issues with component '{source_component}'")

        if not issues:
            self.logger.warning("No issues found for classification")
            return {"status": "success", "issues_processed": 0, "results": []}

        # Classify each issue and update JIRA immediately if enabled
        results: list[ClassificationResult] = []
        total_issues = len(issues)
        update_jira = not dry_run and getattr(args, "update_jira", False)

        for i, issue in enumerate(issues, 1):
            try:
                self.logger.info(f"[{i}/{total_issues}] Processing {issue['key']}...")
                result = self._classify_issue(issue)
                results.append(result)
                self.logger.info(
                    f"[{i}/{total_issues}] Classified {result.issue_key}: "
                    f"{result.original_component} → {result.predicted_component}"
                )

                # Update JIRA immediately if enabled
                if update_jira:
                    self._update_single_jira_component(result)

            except Exception as e:
                self.logger.error(f"[{i}/{total_issues}] Failed to classify {issue['key']}: {e}")

        # Generate summary statistics
        summary = self._generate_summary(results)

        return {
            "status": "success",
            "llm_provider": self.llm_provider,
            "llm_model": self.llm_model,
            "source_component": source_component,
            "issues_processed": len(results),
            "results": [r.__dict__ for r in results],
            "summary": summary,
        }

    def _fetch_issues_by_component(self, project_key: str, component_name: str) -> list[dict]:
        """Fetch JIRA issues by component using JQL."""
        jql = f'project = "{project_key}" AND component = "{component_name}"'
        fields = "summary,description,components"

        try:
            issues = self.jira_assistant.fetch_issues_by_jql(jql, fields=fields)
            return issues
        except Exception as e:
            self.logger.error(f"Failed to fetch issues: {e}", exc_info=True)
            raise

    def _classify_issue(self, issue: dict) -> ClassificationResult:
        """Classify a single issue using LLM."""
        issue_key = issue["key"]
        fields = issue["fields"]
        title = fields.get("summary", "")
        raw_description = fields.get("description", "")

        # Handle ADF (Atlassian Document Format) or plain text
        description = self._extract_text_from_description(raw_description) or "No description"

        # Get original component
        components = fields.get("components", [])
        original_component = components[0]["name"] if components else "None"

        # Build LLM prompt
        prompt = self._build_classification_prompt(title, description)

        # Call LLM using new system_instruction field
        request = LLMRequest(
            messages=[
                LLMMessage(role="user", content=prompt),
            ],
            model=self.llm_model,
            max_completion_tokens=200,
            system_instruction="You are a software component classifier.",
        )

        response = self.llm_client.chat_completion(request)
        predicted_component = response.content.strip()

        # Validate prediction against target components and get component ID
        component_map = {c["name"]: c.get("jira_component_id", "") for c in self.config["components"]}

        if predicted_component not in component_map:
            self.logger.warning(
                f"LLM returned invalid component '{predicted_component}', defaulting to first component"
            )
            # Default to first component if invalid
            first_component = self.config["components"][0]
            predicted_component = first_component["name"]
            jira_component_id = first_component.get("jira_component_id", "")
        else:
            jira_component_id = component_map[predicted_component]

        return ClassificationResult(
            issue_key=issue_key,
            issue_title=title,
            original_component=original_component,
            predicted_component=predicted_component,
            jira_component_id=jira_component_id,
        )

    def _extract_text_from_description(self, description: Any) -> str:
        """Extract plain text from JIRA description (handles both string and ADF).

        Args:
            description: JIRA description (string or dict)

        Returns:
            Plain text description
        """
        if isinstance(description, str):
            return description

        if not isinstance(description, dict) or "content" not in description:
            return ""

        text_parts = []
        for node in description.get("content", []):
            if node.get("type") == "paragraph":
                for child in node.get("content", []):
                    if child.get("type") == "text":
                        text_parts.append(child.get("text", ""))
                text_parts.append("\n")
            elif node.get("type") == "text":
                text_parts.append(node.get("text", ""))

        return "".join(text_parts).strip()

    def _build_classification_prompt(self, title: str, description: str) -> str:
        """Build classification prompt from template enriched with component information.

        Args:
            title: Issue title
            description: Issue description (truncated to 1000 chars)

        Returns:
            Complete classification prompt
        """
        template = self.config["classification_prompt_template"]
        components = self.config["components"]

        # Build component list string
        component_list = ", ".join([c["name"] for c in components])

        # Build domain rules from components (sorted by priority)
        sorted_components = sorted(components, key=lambda x: x["priority"])
        domain_rules_parts = []
        for component in sorted_components:
            priority = component["priority"]
            name = component["name"]
            indicators = "\n   - ".join(component.get("indicators", []))
            notes_str = "\n   - " + "\n   - ".join(component.get("notes", [])) if component.get("notes") else ""

            rule = f"{priority}) **{name}**: Use when the issue involves:\n   - {indicators}{notes_str}"
            domain_rules_parts.append(rule)

        domain_rules = "\n\n".join(domain_rules_parts)

        # Build constraints from all components
        all_constraints = []
        for component in components:
            all_constraints.extend(component.get("constraints", []))

        # Remove duplicates while preserving order
        seen = set()
        unique_constraints = []
        for constraint in all_constraints:
            if constraint not in seen:
                seen.add(constraint)
                unique_constraints.append(constraint)

        constraints_str = "\n".join([f"- {c}" for c in unique_constraints])

        # Format the template with enriched data
        return template.format(
            component_list=component_list,
            domain_rules=domain_rules,
            constraints=constraints_str,
            title=title,
            description=description[:1000],
        )

    def _generate_summary(self, results: list[ClassificationResult]) -> dict[str, Any]:
        """Generate classification statistics."""
        total = len(results)
        if total == 0:
            return {}

        # Count classifications by target component
        component_counts: dict[str, int] = {}
        for result in results:
            component_counts[result.predicted_component] = component_counts.get(result.predicted_component, 0) + 1

        return {
            "total_issues": total,
            "component_distribution": component_counts,
            "unique_components": len(component_counts),
        }

    def _update_single_jira_component(self, result: ClassificationResult) -> None:
        """Update a single JIRA issue with new component.

        Args:
            result: Classification result containing issue key and component ID
        """
        try:
            if not result.jira_component_id:
                self.logger.warning(f"Skipping {result.issue_key}: no component ID found")
                return

            self.jira_assistant.update_issue_components(result.issue_key, result.jira_component_id)
            self.logger.info(
                f"✓ Updated {result.issue_key} to component '{result.predicted_component}' (ID: {result.jira_component_id})"
            )
        except Exception as e:
            self.logger.error(f"✗ Failed to update {result.issue_key}: {e}", exc_info=True)

    def _update_jira_components(self, results: list[ClassificationResult]) -> None:
        """Update JIRA issues with new components (batch operation - DEPRECATED, kept for compatibility)."""
        self.logger.info("Updating JIRA issues with new components...")

        for result in results:
            self._update_single_jira_component(result)
