from argparse import ArgumentParser, Namespace
from utils.command.base_command import BaseCommand
from domains.syngenta.jira.jira_processor import JiraProcessor
from utils.data.json_manager import JSONManager
from utils.logging.logging_manager import LogManager

_logger = LogManager.get_instance().get_logger("ListCustomFieldsCommand")


class ListCustomFieldsCommand(BaseCommand):
    @staticmethod
    def get_name() -> str:
        return "list-custom-fields"

    @staticmethod
    def get_description() -> str:
        return "Lists custom fields available for a JIRA project."

    @staticmethod
    def get_help() -> str:
        return (
            "This command retrieves and lists all custom fields available for "
            "a specified JIRA project."
        )

    @staticmethod
    def get_arguments(parser: ArgumentParser):
        parser.add_argument(
            "--output-file",
            required=False,
            help="The file path to save the custom fields in JSON format.",
        )

    @staticmethod
    def main(args: Namespace):
        """
        Main function to fetch and save custom fields for a given Jira project.

        Args:
            args (Namespace): Command-line arguments.
        """
        output_file = args.output_file if args.output_file else "custom_fields.json"

        try:
            # Initialize JiraProcessor or equivalent class
            jira_processor = JiraProcessor()

            # Fetch all custom fields
            custom_fields = jira_processor.fetch_custom_fields()

            if not custom_fields:
                ListCustomFieldsCommand._logger.info("No custom fields available in Jira.")
                _logger.info("No custom fields available in Jira.")
                return

            # Save the custom fields to a JSON file using JSONManager
            JSONManager.write_json(custom_fields, output_file)
            _logger.info(f"Custom fields successfully saved to '{output_file}'.")

        except Exception as e:
            ListCustomFieldsCommand._logger.error(f"Failed to fetch custom fields: {e}")
            print(f"Error fetching custom fields: {e}")
