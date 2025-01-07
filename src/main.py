from utils.error_manager import handle_generic_exception
from log_config import LogManager
import os
from utils.command_manager import CommandManager

logger = LogManager.get_instance().get_logger("CLI")


def main():
    """
    Entry point for the CLI application. Loads commands dynamically and executes
    the requested command.
    """

    command_manager = CommandManager(os.path.join(os.path.dirname(__file__), "domains"))
    command_manager.load_commands()
    parser = command_manager.build_parser()

    # Parse command-line arguments
    args, unknown = parser.parse_known_args()

    # Handle global or hierarchical help
    if "help" in unknown:
        parser.print_help()
        return

    # If no command is provided, show general help
    if not hasattr(args, "func") or args.func is None:
        parser.print_help()
        return

    # Execute the command
    try:
        args.func(args)
    except Exception as e:
        handle_generic_exception(e, "An error occurred during execution.")


if __name__ == "__main__":
    main()
