from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace


class BaseCommand(ABC):
    """Abstract base class for all CLI commands, supporting hierarchical structure."""

    @staticmethod
    @abstractmethod
    def get_name() -> str:
        """Returns the name of the command (or domain/subdomain).

        Returns:
            str: The name of the command.
        """
        pass

    @staticmethod
    def get_description() -> str:
        """Returns a description of the command (or domain/subdomain).

        Returns:
            str: The description of the command.
        """
        return "No description provided."

    @staticmethod
    def get_help() -> str:
        """Returns help text for the command (or domain/subdomain).

        Returns:
            str: The help text of the command.
        """
        return "No help available."

    @classmethod
    def register_command(cls, parent_parser):
        """Registers the command or subdomain in the given parent parser/subparsers.

        Args:
            parent_parser (ArgumentParser or _SubParsersAction): The parser or subparsers to add
                                                                 the command to.
        """
        parser = parent_parser.add_parser(
            cls.get_name(),
            description=cls.get_description(),
            help=cls.get_help(),
        )
        cls.get_arguments(parser)

        # Add help command for this command
        parser.set_defaults(func=cls.main)

    @staticmethod
    @abstractmethod
    def get_arguments(parser: ArgumentParser):
        """Adds arguments to the parser.

        Args:
            parser (ArgumentParser): The parser to which arguments are added.
        """
        pass

    @staticmethod
    def add_subcommands(parser: ArgumentParser):
        """Adds subcommands to the given parser. Override this method if subcommands exist.

        Args:
            parser (ArgumentParser): The parser to which subcommands are added.
        """
        pass

    @staticmethod
    @abstractmethod
    def main(args: Namespace):
        """Executes the main logic of the command.

        Args:
            args (Namespace): Parsed arguments from the CLI.
        """
        pass
