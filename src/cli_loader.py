import importlib
import os
import inspect
import pkgutil
from argparse import ArgumentParser
from utils.base_command import BaseCommand
from log_config import LogManager

logger = LogManager.get_instance().get_logger("CLI.Loader")


def load_commands(parser: ArgumentParser, base_path: str):
    """
    Dynamically loads all commands that inherit BaseCommand from a given base directory.

    Args:
        parser (ArgumentParser): Root CLI parser.
        base_path (str): Base directory to search for commands.
    """
    base_path = os.path.abspath(base_path)
    if not os.path.isfile(os.path.join(base_path, "__init__.py")):
        raise ValueError(f"Base path {base_path} is not a valid Python package.")
    logger.info(f"Loading commands from base path: {base_path}")

    for root, _, _ in os.walk(base_path):
        if not os.path.isfile(os.path.join(root, "__init__.py")):
            logger.debug(f"Skipping non-package directory: {root}")
            continue

        for _, module_name, _ in pkgutil.iter_modules([root]):
            module_path = _module_path_from_root(base_path, root, module_name)
            try:
                # Import module using relative module path and package
                module = importlib.import_module(module_path, package="domains")
                logger.debug(f"Successfully imported module: {module_path}")

                for _, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BaseCommand) and obj is not BaseCommand:
                        # Configura o parser para a classe encontrada
                        command_parser = parser.add_subparsers(dest="command").add_parser(
                            module_name, help=obj.__doc__
                        )
                        obj.get_arguments(command_parser)
                        command_parser.set_defaults(func=obj.main)
                        logger.info(f"Loaded command: {module_name}")
                        break
            except ImportError as e:
                logger.error(f"ImportError for module {module_path}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Unexpected error loading module {module_path}: {e}", exc_info=True)


def _module_path_from_root(base_path: str, root: str, module_name: str) -> str:
    """
    Constructs the relative module path for importlib.import_module.

    Args:
        base_path (str): Base directory of the project.
        root (str): Current directory being processed.
        module_name (str): Name of the module.

    Returns:
        str: Relative module path for importlib.
    """
    # Calculate the relative path from base_path to the current root
    relative_path = os.path.relpath(root, base_path)
    # Replace path separators with dots to create the module's package path
    if relative_path == ".":
        # If the root is the same as the base_path, it’s a top-level module
        return f".{module_name}"
    else:
        # Construct the full module path for subdirectories
        package_path = relative_path.replace(os.sep, ".")
        return f".{package_path}.{module_name}"
