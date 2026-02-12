import importlib
import inspect
import os
import pkgutil
from argparse import ArgumentParser, _SubParsersAction
from types import ModuleType

from utils.command.base_command import BaseCommand
from utils.command.optional_command import OptionalCommand
from utils.file_manager import FileManager
from utils.logging.logging_manager import LogManager

from .error import (
    CommandLoadError,
    CommandManagerError,
    HierarchyConflictError,
    ModuleImportError,
)


class CommandManager:
    _logger = LogManager.get_instance().get_logger("CommandManager")

    def __init__(self, base_path: str):
        self.base_path = os.path.abspath(base_path)
        self.hierarchy: dict[str, dict] = {}

    def load_commands(self) -> None:
        """Dynamically loads all commands inheriting BaseCommand and builds the hierarchy."""
        self._logger.debug(f"Starting to load commands from base path: {self.base_path}")

        for root, _, _ in os.walk(self.base_path):
            if not os.path.isfile(os.path.join(root, "__init__.py")):
                self._logger.debug(f"Skipping non-package directory: {root}")
                continue

            for _, module_name, _ in pkgutil.iter_modules([root]):
                try:
                    absolute_module_path = os.path.join(root, module_name)
                    if FileManager.is_folder(absolute_module_path) and not os.path.isfile(
                        os.path.join(absolute_module_path, "__init__.py")
                    ):
                        self._logger.debug(f"Skipping non-package module: {absolute_module_path}")
                        continue

                    self._logger.debug(f"Processing module: {module_name} in {root}")
                    module = self._import_module(root, module_name)
                    self._process_module(module)
                except (CommandManagerError, Exception) as e:
                    self._logger.error(str(e), exc_info=True)

        self._logger.debug("Finished loading commands.")

    def _module_path_from_root(self, root: str, module_name: str) -> str:
        """Constructs the relative module path for importlib.import_module.

        Args:
            root (str): Current directory being processed.
            module_name (str): Name of the module.

        Returns:
            str: Relative module path for importlib.
        """
        # Calculate the relative path from base_path to the current root
        relative_path = os.path.relpath(root, self.base_path)
        # Replace path separators with dots to create the module's package path
        if relative_path == ".":
            # If the root is the same as the base_path, it’s a top-level module
            return f".{module_name}"
        else:
            # Construct the full module path for subdirectories
            package_path = relative_path.replace(os.sep, ".")
            return f".{package_path}.{module_name}"

    def _import_module(self, root: str, module_name: str):
        """Imports a module dynamically."""
        relative_path = self._module_path_from_root(root, module_name)
        self._logger.debug(f"Importing module {relative_path}")
        try:
            return importlib.import_module(relative_path, package="domains")
        except ImportError as e:
            raise ModuleImportError(module_path=relative_path, error=e) from e
        except Exception as e:
            raise ModuleImportError(module_path=relative_path, error=e) from e

    def _process_module(self, module):
        """Processes a module to find BaseCommand subclasses and updates the hierarchy."""
        self._logger.debug(f"Inspecting module: {module.__name__}")
        try:
            module_members = inspect.getmembers(module, inspect.isclass)
            if len(module_members) == 0:
                self._add_to_hierarchy(module)
                return

            for name, obj in module_members:
                if issubclass(obj, BaseCommand) and obj is not BaseCommand:
                    self._logger.debug(f"Found command class: {name}")

                    # Skip OptionalCommand if dependencies are not available
                    if issubclass(obj, OptionalCommand):
                        if not obj.can_register():
                            self._logger.debug(f"Skipping optional command {name} due to missing dependencies")
                            continue

                    # Validate compliance with BaseCommand
                    missing_methods = [
                        method
                        for method in BaseCommand.__abstractmethods__
                        if getattr(obj, method) == getattr(BaseCommand, method)
                    ]

                    if missing_methods:
                        self._logger.debug(
                            f"Command {name} is missing required methods: {missing_methods} and will be skipped."
                        )
                        continue

                    self._add_to_hierarchy(obj)
        except Exception as e:
            raise CommandLoadError(module_name=module.__name__, error=e) from e

    def _add_to_hierarchy(self, entity: type[BaseCommand] | ModuleType):
        """Adds a command class to the hierarchy based on its domain and subdomain."""
        try:
            if isinstance(entity, type) and issubclass(entity, BaseCommand):
                name_parts = entity.__module__.split(".")
                name_parts.pop(0)  # Remove the "domains" part
                command_name = entity.get_name()

                if command_name in self.hierarchy:
                    raise HierarchyConflictError(command_name=command_name)

                current_level = self.hierarchy
                for part in name_parts[:-1]:
                    current_level = current_level.setdefault(part, {})

                current_level[command_name] = {
                    "name": command_name,
                    "description": entity.get_description(),
                    "help": entity.get_help(),
                    "class": entity,
                }
                self._logger.debug(f"Command {command_name} added successfully.")
            elif isinstance(entity, ModuleType):
                self._logger.debug(f"Treating {entity.__name__} as a subdomain.")
        except Exception as e:
            raise CommandLoadError(module_name=entity.__module__, error=e) from e

    def build_parser(self) -> ArgumentParser:
        """Builds the ArgumentParser hierarchy from the loaded command structure."""
        self._logger.debug("Building argument parser hierarchy")
        try:
            parser = ArgumentParser(
                prog="pytoolkit",
                description="pytoolkit CLI - A command-line toolkit",
                add_help=False,
            )
            subparsers = parser.add_subparsers(dest="domain", help="Available domains")

            for domain_name, substructure in self.hierarchy.items():
                self._logger.debug(f"Adding domain to parser: {domain_name}")
                self._add_subparser(subparsers, domain_name, substructure)

            return parser
        except Exception as e:
            raise CommandManagerError(message="Failed to build argument parser hierarchy", error=e)

    def _add_subparser(self, subparsers: _SubParsersAction, name: str, substructure: dict):
        """Recursively adds subparsers for domains, subdomains, and commands."""
        self._logger.debug(f"Creating parser for: {name}")
        try:
            if isinstance(substructure, dict) and "class" not in substructure:
                parser = subparsers.add_parser(name, help=f"{name} commands")
                parser_subparsers = parser.add_subparsers(dest="subdomain_or_command", help=f"{name} subcommands")

                for key, value in substructure.items():
                    self._add_subparser(parser_subparsers, key, value)
            else:
                command_metadata = substructure
                self._logger.debug(f"Registering command: {command_metadata['name']}")
                command_metadata["class"].register_command(subparsers)
        except Exception as e:
            raise CommandManagerError(
                message=f"Failed to add subparser for {name}",
                substructure=substructure,
                error=e,
            )
