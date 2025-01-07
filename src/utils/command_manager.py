import os
import inspect
import importlib
import pkgutil
from argparse import ArgumentParser, _SubParsersAction
from typing import Dict, Type, Union
from types import ModuleType
from utils.base_command import BaseCommand
from utils.logging_manager import LogManager
from utils.file_manager import FileManager


class CommandManager:
    _logger = LogManager.get_instance().get_logger("CommandManager")

    def __init__(self, base_path: str):
        self.base_path = os.path.abspath(base_path)
        self.hierarchy: Dict[str, Dict] = {}

    def load_commands(self) -> None:
        """
        Dynamically loads all commands inheriting BaseCommand and builds the hierarchy.
        """
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
                except Exception as e:
                    self._logger.error(f"Failed to load module {module_name}: {e}", exc_info=True)

        self._logger.debug("Finished loading commands.")

    def _import_module(self, root: str, module_name: str):
        """
        Imports a module dynamically.
        """
        relative_path = self._module_path_from_root(root, module_name)
        self._logger.debug(f"Importing module {relative_path}")
        try:
            return importlib.import_module(relative_path, package="domains")
        except ImportError as e:
            self._logger.error(f"ImportError for module {relative_path}: {e}", exc_info=True)
        except Exception as e:
            self._logger.error(
                f"Unexpected error loading module {relative_path}: {e}",
                exc_info=True,
            )

    def _process_module(self, module):
        """
        Processes a module to find BaseCommand subclasses and updates the hierarchy,
        ensuring they fully comply with the BaseCommand interface.
        """
        self._logger.debug(f"Inspecting module: {module.__name__}")
        try:
            module_members = inspect.getmembers(module, inspect.isclass)
            if len(module_members) == 0:
                self._add_to_hierarchy(module)
                return
            for name, obj in module_members:
                if issubclass(obj, BaseCommand) and obj is not BaseCommand:
                    self._logger.debug(f"Found command class: {name}")

                    # Validate compliance with BaseCommand
                    missing_methods = []
                    for method in BaseCommand.__abstractmethods__:
                        # Check if the subclass has overridden the method
                        if getattr(obj, method) == getattr(BaseCommand, method):
                            missing_methods.append(method)

                    if missing_methods:
                        self._logger.debug(
                            f"Command {name} is missing required methods: {missing_methods} "
                            "and will be skipped."
                        )
                        continue

                    self._add_to_hierarchy(obj)
        except Exception as e:
            self._logger.error(f"Failed to process module {module.__name__}: {e}", exc_info=True)

    def _add_to_hierarchy(self, entity: Union[Type[BaseCommand], ModuleType]):
        """
        Adds a command class to the hierarchy based on its domain and subdomain.
        """
        try:
            if isinstance(entity, type) and issubclass(entity, BaseCommand):
                name_parts = entity.__module__.split(".")
                name_parts.pop(0)  # Remove the "domains" part
                command_name = entity.get_name()
                module_name = name_parts[len(name_parts) - 2]

                self._logger.debug(f"Adding command {command_name} to {module_name}")

                i = 0
                current_level = self.hierarchy
                while name_parts[i] != entity.get_name():
                    if name_parts[i] not in current_level:
                        self._logger.debug(f"Creating new domain/subdomain: {name_parts[i]}")
                        current_level[name_parts[i]] = {}
                    current_level = current_level[name_parts[i]]
                    i += 1

                if command_name in current_level:
                    self._logger.error(f"Duplicate command detected: {command_name}")
                    raise ValueError(f"Duplicate command: {command_name}")

                current_level[command_name] = {
                    "name": entity.get_name(),
                    "description": entity.get_description(),
                    "help": entity.get_help(),
                    "class": entity,
                }
                self._logger.debug(f"Command {command_name} added to {module_name} successfully")
            elif isinstance(entity, ModuleType):
                # Treat as subdomain if necessary
                modules = entity.__name__.split(".")
                if "domains" in modules:
                    domain_name = modules[modules.index("domains") + 1]
                    if domain_name not in self.hierarchy:
                        self.hierarchy[domain_name] = {}
                        self._logger.debug(f"Domain {domain_name} created successfully")

                    subdomain_name = (
                        modules[modules.index("domains") + 2] if len(modules) > 2 else None
                    )
                    if subdomain_name:
                        if subdomain_name not in self.hierarchy[domain_name]:
                            self.hierarchy[domain_name][subdomain_name] = {}
                        self._logger.debug(
                            f"Subdomain {subdomain_name} added successfully in {domain_name}"
                        )

        except Exception as e:
            self._logger.error(f"Failed to add command to hierarchy: {e}")

    def build_parser(self) -> ArgumentParser:
        """
        Builds the ArgumentParser hierarchy from the loaded command structure.
        """
        self._logger.debug("Building argument parser hierarchy")
        try:
            parser = ArgumentParser(
                prog="pytoolkit",
                description="pytoolkit CLI - A command-line toolkit",
                add_help=False,  # Disable default help to add custom help logic
            )
            subparsers = parser.add_subparsers(dest="domain", help="Available domains")

            for domain_name, substructure in self.hierarchy.items():
                self._logger.debug(f"Adding domain to parser: {domain_name}")
                self._add_subparser(subparsers, domain_name, substructure)

            # Add global help command
            parser.add_argument("help", nargs="?", help="Show help message")

            self._logger.debug("Argument parser hierarchy built successfully")
            return parser
        except Exception as e:
            self._logger.error(f"Failed to build argument parser hierarchy: {e}")

    def _add_subparser(self, subparsers: _SubParsersAction, name: str, substructure: Dict):
        """
        Recursively adds subparsers for domains, subdomains, and commands.
        """
        self._logger.debug(f"Creating parser for: {name}")

        try:
            if isinstance(substructure, dict) and "class" not in substructure:
                # This is a domain or subdomain
                parser = subparsers.add_parser(name, help=f"{name} commands")
                parser_subparsers = parser.add_subparsers(
                    dest="subdomain_or_command", help=f"{name} subcommands"
                )

                for key, value in substructure.items():
                    self._add_subparser(parser_subparsers, key, value)
            else:
                # This is a command
                command_metadata = substructure
                self._logger.debug(f"Registering command: {command_metadata['name']}")
                command_metadata["class"].register_command(subparsers)
        except Exception as e:
            self._logger.error(f"Failed to add subparser for {name}: {e}")

    def _module_path_from_root(self, root: str, module_name: str) -> str:
        """
        Constructs the relative module path for importlib.import_module.

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
