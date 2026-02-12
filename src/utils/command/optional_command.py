"""Optional command base for dependency-gated commands.

This module provides a base class for commands that require optional
dependencies. Commands inheriting from OptionalCommand will only be
registered if their required dependencies are available.
"""

from utils.command.base_command import BaseCommand
from utils.dependencies import is_available
from utils.logging.logging_manager import LogManager


class OptionalCommand(BaseCommand):
    """Base class for commands requiring optional dependencies.

    Commands inheriting from this class will only be registered
    if their required dependencies are available. If dependencies are
    missing, the command is silently skipped during discovery with a
    warning log message.

    Example:
        class MyMLCommand(OptionalCommand):
            REQUIRED_GROUP = "ml"
            REQUIRED_MODULES = ["sklearn", "scipy"]

            @staticmethod
            def get_name() -> str:
                return "my_ml_command"

            # ... implement other required methods
    """

    # Subclasses override these
    REQUIRED_GROUP: str = None  # e.g., "ml", "llm"
    REQUIRED_MODULES: list[str] = []  # e.g., ["sklearn", "scipy"]

    @classmethod
    def is_available(cls) -> bool:
        """Check if all required optional dependencies are installed.

        Returns:
            True if all required modules are available, False otherwise.
        """
        if not cls.REQUIRED_MODULES:
            return True
        return all(is_available(mod) for mod in cls.REQUIRED_MODULES)

    @classmethod
    def can_register(cls) -> bool:
        """Determine if command should be registered with CommandManager.

        Logs a warning message if dependencies are missing.

        Returns:
            True if command should be registered, False otherwise.
        """
        available = cls.is_available()
        if not available:
            logger = LogManager.get_instance().get_logger("CommandManager")
            logger.warning(
                f"Command '{cls.get_name()}' requires optional dependencies "
                f"'{cls.REQUIRED_GROUP}' and will be skipped. "
                f"Install with: pip install -e '.[{cls.REQUIRED_GROUP}]'"
            )
        return available
