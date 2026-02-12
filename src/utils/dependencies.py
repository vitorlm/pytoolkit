"""Centralized optional dependency management.

This module provides a consistent pattern for handling optional
dependencies across PyToolkit, replacing scattered try-except blocks.

Pattern inspired by pandas.compat._optional and sklearn's import handling.
"""

from importlib import import_module
from functools import lru_cache
from typing import Any


class OptionalDependencyError(ImportError):
    """Raised when optional dependency is missing with helpful install message."""

    def __init__(self, name: str, install_group: str) -> None:
        self.name = name
        self.install_group = install_group
        super().__init__(
            f"Optional dependency '{name}' is not installed.\n\n"
            f"Install with: pip install -e '.[{install_group}]'\n\n"
            f"Documentation: https://github.com/user/PyToolkit#optional-dependencies"
        )


@lru_cache(maxsize=None)
def require_optional(name: str, group: str = "ml") -> Any:
    """Import an optional dependency with clear error messaging.

    This caches successful imports and raises OptionalDependencyError with
    helpful messages on failure.

    Args:
        name: Module name to import (e.g., 'sklearn', 'google.genai', 'utils.llm.portkey_adapter')
        group: Optional dependency group for install hint (default: 'ml')

    Returns:
        The imported module

    Raises:
        OptionalDependencyError: With helpful install message

    Example:
        >>> sklearn = require_optional('sklearn', 'ml')
        >>> sentence_transformers = require_optional('sentence_transformers', 'ml')
    """
    try:
        return import_module(name)
    except ImportError as e:
        raise OptionalDependencyError(name, group) from e


def is_available(name: str) -> bool:
    """Check if optional dependency is available without importing.

    Useful for feature flags and conditional command registration.

    Args:
        name: Module name to check

    Returns:
        True if module can be imported, False otherwise

    Example:
        >>> if is_available('sklearn'):
        ...     from sklearn.metrics import accuracy_score
    """
    try:
        import_module(name)
        return True
    except (ImportError, ModuleNotFoundError):
        return False


# Convenience constants for commonly checked optional dependencies
# These are computed once at module load time
SKLEARN_AVAILABLE = is_available("sklearn")
SCIPY_AVAILABLE = is_available("scipy")
SENTENCE_TRANSFORMERS_AVAILABLE = is_available("sentence_transformers")
TORCH_AVAILABLE = is_available("torch")
TRANSFORMERS_AVAILABLE = is_available("transformers")
GEMINI_AVAILABLE = is_available("google.genai")
PORTKEY_AVAILABLE = is_available("portkey")
ZAI_AVAILABLE = is_available("zai")
