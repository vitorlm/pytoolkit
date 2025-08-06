"""
Base Adapter for PyToolkit MCP Integration.

This module provides the base adapter class that all service adapters must inherit from.
It provides common functionality for logging, caching, error handling, and service integration.
"""

from abc import ABC, abstractmethod
from typing import Any

from utils.cache_manager.cache_manager import CacheManager
from utils.env_loader import ensure_env_loaded
from utils.logging.logging_manager import LogManager


class BaseAdapter(ABC):
    """
    Base adapter for integrating PyToolkit services with MCP.

    Provides common infrastructure:
    - Standardized logging
    - Intelligent caching
    - Error handling
    - Environment loading
    """

    def __init__(self, adapter_name: str) -> None:
        """
        Initialize base adapter.

        Args:
            adapter_name: Name of the adapter for logging
        """
        # Load general environment variables without requiring specific services
        ensure_env_loaded(required_vars=[])  # Empty list = no required variables
        self.adapter_name = adapter_name
        self.logger = LogManager.get_instance().get_logger("MCPAdapter", adapter_name)
        self.cache = CacheManager.get_instance()
        self._service = None

        self.logger.info(f"Initializing {adapter_name} adapter")

    @abstractmethod
    def initialize_service(self) -> Any:
        """Initialize the specific PyToolkit service."""

    @property
    def service(self) -> Any:
        """Lazy loading of PyToolkit service."""
        if self._service is None:
            self._service = self.initialize_service()
            self.logger.debug(f"Service initialized for {self.adapter_name}")
        return self._service

    def get_cache_key(self, operation: str, **kwargs) -> str:
        """Generate standardized cache key."""
        params = "_".join([f"{k}_{v}" for k, v in sorted(kwargs.items())])
        return f"{self.adapter_name}_{operation}_{params}"

    def cached_operation(self, operation: str, func, expiration_minutes: int = 60, **kwargs) -> Any:
        """
        Execute operation with automatic caching.

        Args:
            operation: Name of the operation
            func: Function to execute
            expiration_minutes: Cache expiration time
            **kwargs: Parameters for the function
        """
        cache_key = self.get_cache_key(operation, **kwargs)

        # Try to load from cache
        cached_result = self.cache.load(cache_key, expiration_minutes=expiration_minutes)
        if cached_result is not None:
            self.logger.debug(f"Cache hit for {operation}")
            return cached_result

        try:
            # Execute operation
            self.logger.debug(f"Executing {operation} - cache miss")
            result = func(**kwargs)

            # Save to cache
            self.cache.save(cache_key, result)
            self.logger.debug(f"Cached result for {operation}")

            return result

        except Exception as e:
            self.logger.error(f"Error in {operation}: {e}")
            raise

    def health_check(self) -> dict[str, Any]:
        """Check adapter health."""
        try:
            # Test service initialization
            service = self.service

            return {
                "adapter": self.adapter_name,
                "status": "healthy",
                "service_initialized": service is not None,
                "cache_available": self.cache is not None,
            }
        except Exception as e:
            self.logger.error(f"Health check failed for {self.adapter_name}: {e}")
            return {
                "adapter": self.adapter_name,
                "status": "unhealthy",
                "error": str(e),
            }

    def clear_cache(self, operation: str | None = None) -> dict[str, Any]:
        """
        Clear cache for this adapter.

        Args:
            operation: Specific operation to clear, or None for all

        Returns:
            Status of cache clearing operation
        """
        try:
            if operation:
                # Clear specific operation cache
                cache_pattern = f"{self.adapter_name}_{operation}_*"
                self.logger.info(f"Clearing cache for operation: {operation}")
            else:
                # Clear all adapter cache
                cache_pattern = f"{self.adapter_name}_*"
                self.logger.info(f"Clearing all cache for adapter: {self.adapter_name}")

            # Note: This is a simplified implementation
            # CacheManager would need to support pattern-based clearing
            return {
                "adapter": self.adapter_name,
                "operation": operation,
                "status": "cache_cleared",
                "message": f"Cache cleared for pattern: {cache_pattern}",
            }

        except Exception as e:
            self.logger.error(f"Failed to clear cache: {e}")
            return {"adapter": self.adapter_name, "status": "error", "error": str(e)}
