from utils.error.base_custom_error import BaseCustomError


class CommandManagerError(BaseCustomError):
    """Base class for all CommandManager errors."""

    pass


class CommandLoadError(CommandManagerError):
    """Raised when a command fails to load."""

    def __init__(self, module_name: str, error: Exception):
        super().__init__(
            f"Failed to load command from module '{module_name}'",
            module_name=module_name,
            original_error=error,
        )


class HierarchyConflictError(CommandManagerError):
    """Raised when a duplicate command name is detected in the hierarchy."""

    def __init__(self, command_name: str):
        super().__init__(f"Duplicate command detected: '{command_name}'", command_name=command_name)


class ModuleImportError(CommandManagerError):
    """Raised when a module fails to import."""

    def __init__(self, module_path: str, error: Exception):
        super().__init__(
            f"Failed to import module '{module_path}'",
            module_path=module_path,
            original_error=error,
        )
