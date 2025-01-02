import argparse
from cli_loader import load_commands
from utils.error_manager import handle_generic_exception
from log_config import LogManager
import os

logger = LogManager.get_instance().get_logger("CLI")


def main():
    """
    Entry point for the CLI application. Loads commands dynamically and executes
    the requested command.
    """
    parser = argparse.ArgumentParser(description="CLI tool for dynamic commands.")
    # Ensure the base path is relative to this script's location
    commands_base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "domains"))

    # Load commands dynamically from the base path
    try:
        load_commands(parser, commands_base_path)
    except ValueError as e:
        logger.error(f"Failed to load commands: {e}")
        return

    # Parse command-line arguments
    args = parser.parse_args()

    # If no command is provided, print the help message
    if not hasattr(args, "func"):
        parser.print_help()
        return

    # Execute the command
    try:
        args.func(args)
    except Exception as e:
        handle_generic_exception(e, "An error occurred during execution.")


if __name__ == "__main__":
    main()
