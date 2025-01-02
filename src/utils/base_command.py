from abc import ABC, abstractmethod
from argparse import ArgumentParser, Namespace


class BaseCommand(ABC):
    """
    Abstract base class for all CLI commands.
    """

    @staticmethod
    @abstractmethod
    def get_arguments(parser: ArgumentParser):
        """
        Adds arguments to the CLI parser.

        Args:
            parser (ArgumentParser): The parser to which arguments are added.
        """
        pass

    @staticmethod
    @abstractmethod
    def main(args: Namespace):
        """
        Executes the main logic of the command.

        Args:
            args (Namespace): Parsed arguments from the CLI.
        """
        pass
