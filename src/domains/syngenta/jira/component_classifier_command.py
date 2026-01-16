import sys
from argparse import ArgumentParser, Namespace

from domains.syngenta.jira.component_classifier_service import ComponentClassifierService
from utils.command.base_command import BaseCommand
from utils.data.json_manager import JSONManager
from utils.env_loader import ensure_env_loaded, load_domain_env
from utils.logging.logging_manager import LogManager


class ComponentClassifierCommand(BaseCommand):
    """Command to classify JIRA issues into components using LLM."""

    @staticmethod
    def get_name() -> str:
        """Get the command name."""
        return "classify-components"

    @staticmethod
    def get_description() -> str:
        """Get the command description."""
        return "Classify JIRA issues into new components using LLM analysis."

    @staticmethod
    def get_help() -> str:
        """Get detailed help information."""
        return """
        This command fetches all JIRA issues associated with a source component,
        analyzes their title and description using an LLM, and classifies them
        into target components defined in configuration.

        Examples:
          # Dry-run classification using z.ai with GLM-4.7 model
          python src/main.py syngenta jira classify-components \\
            --project-key CWS \\
            --source-component "Legacy System" \\
            --llm-provider zai \\
            --llm-model glm-4.7 \\
            --dry-run

          # Classify using Portkey with GPT-4o-mini
          python src/main.py syngenta jira classify-components \\
            --project-key CWS \\
            --source-component "Legacy System" \\
            --llm-provider portkey \\
            --llm-model openai/gpt-4o-mini \\
            --dry-run

          # Classify using z.ai GLM-4.5 and update JIRA
          python src/main.py syngenta jira classify-components \\
            --project-key CWS \\
            --source-component "Legacy System" \\
            --llm-provider zai \\
            --llm-model glm-4.5 \\
            --update-jira \\
            --output output/classification_results.json

        LLM Provider & Model Options:
          - portkey: Syngenta AI Foundry Gateway (requires PORTKEY_API_KEY)
            Models: openai/gpt-4o-mini, openai/gpt-4o, anthropic/claude-3-5-sonnet-20241022
          - zai: z.ai API (requires Z_AI_API_KEY)
            Models: glm-4.7, glm-4.6, glm-4.5, glm-4.5-air, glm-4.5-flash
        """

    @staticmethod
    def get_arguments(parser: ArgumentParser) -> None:
        """Set up command line arguments."""
        parser.add_argument("--project-key", type=str, required=True, help="JIRA project key (e.g., PROJ, CATALOG)")
        parser.add_argument(
            "--source-component", type=str, required=True, help="Name of the source component to classify from"
        )
        parser.add_argument(
            "--output",
            type=str,
            required=False,
            default="output/component_classification.json",
            help="Output file path for classification results (default: output/component_classification.json)",
        )
        parser.add_argument("--dry-run", action="store_true", help="Run classification without updating JIRA issues")
        parser.add_argument(
            "--update-jira",
            action="store_true",
            help="Update JIRA issues with classified components (requires --no-dry-run)",
        )
        parser.add_argument(
            "--llm-provider",
            type=str,
            required=True,
            choices=["portkey", "zai", "gemini"],
            help="LLM provider: 'portkey' (Syngenta AI Foundry Gateway), 'zai' (z.ai API), or 'gemini' (Gemini API)",
        )
        parser.add_argument(
            "--llm-model",
            type=str,
            required=True,
            help="LLM model name. Portkey: 'openai/gpt-4o-mini', 'anthropic/claude-3-5-sonnet'. zai: 'glm-4.7'",
        )

    @staticmethod
    def main(args: Namespace) -> None:
        """Execute the command logic."""
        ensure_env_loaded()  # MANDATORY first line
        load_domain_env("domains/syngenta/jira")

        logger = LogManager.get_instance().get_logger("ComponentClassifierCommand")

        try:
            logger.info(
                f"Starting component classification for project={args.project_key}, component={args.source_component}"
            )

            # Log LLM configuration being used
            logger.info(f"Using LLM provider: {args.llm_provider}")
            logger.info(f"Using LLM model: {args.llm_model}")

            # Execute service
            service = ComponentClassifierService(llm_provider=args.llm_provider, llm_model=args.llm_model)
            result = service.execute(args)

            # Save results to file
            JSONManager.write_json(args.output, result)
            logger.info(f"Classification results saved to: {args.output}")

            # Print summary
            summary = result.get("summary", {})
            print("\n=== Classification Summary ===")
            print(f"LLM Provider: {args.llm_provider}")
            print(f"LLM Model: {args.llm_model}")
            print(f"Total issues processed: {result['issues_processed']}")

            if result.get("results"):
                print("\n=== Detailed Classification Results ===")
                print(f"{'Issue Key':<15} | {'Old Component':<20} | {'New Component':<20} | {'Summary'}")
                print("-" * 110)
                for res in result["results"]:
                    key = res.get("issue_key", "N/A")
                    old = res.get("original_component", "N/A")
                    new = res.get("predicted_component", "N/A")
                    summary_text = res.get("issue_title", "N/A")
                    if len(summary_text) > 50:
                        summary_text = summary_text[:47] + "..."
                    print(f"{key:<15} | {old:<20} | {new:<20} | {summary_text}")

            print("\nComponent Distribution:")
            for component, count in summary.get("component_distribution", {}).items():
                print(f"  {component}: {count}")

            if args.dry_run:
                print("\n[DRY RUN] No JIRA issues were updated. Use --update-jira to apply changes.")
            elif not args.update_jira:
                print("\n[INFO] Used classification only. Use --update-jira to apply changes.")
            else:
                print("\n[SUCCESS] JIRA issues have been updated with new components.")

        except Exception as e:
            logger.error(f"Command failed: {e}", exc_info=True)
            sys.exit(1)
