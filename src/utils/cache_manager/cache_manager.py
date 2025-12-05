import hashlib
import os
from typing import Any

from log_config import log_manager
from utils.cache_manager.error import CacheManagerError
from utils.cache_manager.file_cache import FileCacheBackend
from utils.data.json_manager import JSONManager


class CacheManager:
    """A flexible CacheManager for managing cache data with a singleton pattern.
    Supports file-based caching and is extensible for other backends (e.g., Redis).
    """

    _instance = None  # Singleton instance
    _logger = log_manager.get_logger("CacheManager")

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CacheManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, cache_backend: str = "file", cache_dir: str | None = None):
        """Initializes the CacheManager. This follows a singleton pattern.

        Args:
            cache_backend (str): Backend type for caching (default: "file").
            cache_dir (Optional[str]): Directory for file-based caching.

        Raises:
            CacheManagerError: If the backend is unsupported.
        """
        if getattr(self, "_initialized", False):
            return  # Avoid reinitialization

        self.cache_backend = cache_backend
        self._logger.info(f"Initializing CacheManager with backend: {cache_backend}")

        # Dynamically initialize the backend
        self._backend = self._initialize_backend(cache_backend, cache_dir)

        self._initialized = True

    def _initialize_backend(self, cache_backend: str, cache_dir: str | None = None):
        """Initializes the specified cache backend.

        Args:
            cache_backend (str): Backend type (e.g., "file").
            cache_dir (Optional[str]): Directory for file-based caching.

        Returns:
            CacheBackend: The initialized cache backend.

        Raises:
            CacheManagerError: If the backend type is unsupported.
        """
        try:
            if cache_backend == "file":
                cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), "../../../cache")

                # Ensure the cache directory exists
                os.makedirs(cache_dir, exist_ok=True)

                return FileCacheBackend(cache_dir)

            raise CacheManagerError(f"Unsupported cache backend: {cache_backend}", backend=cache_backend)
        except Exception as e:
            self._logger.error(f"Failed to initialize backend '{cache_backend}': {e}")
            raise CacheManagerError(
                f"Failed to initialize backend '{cache_backend}'",
                backend=cache_backend,
                cache_dir=cache_dir,
                error=str(e),
            )

    @classmethod
    def get_instance(cls, *args, **kwargs):
        """Get the singleton instance of the CacheManager.

        Args:
            *args: Positional arguments for CacheManager initialization.
            **kwargs: Keyword arguments for CacheManager initialization.

        Returns:
            CacheManager: The singleton instance of CacheManager.
        """
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def load(self, key: str, expiration_minutes: int | None = None) -> Any | None:
        """Load data from the cache using the provided key.

        Args:
            key (str): The cache key to retrieve data for.
            expiration_minutes (Optional[int]): Expiration time in minutes.

        Returns:
            Optional[Any]: The cached data if valid, otherwise None.
        """
        try:
            self._logger.debug(f"Loading cache for key: {key}")
            return self._backend.load(key, expiration_minutes)
        except Exception as e:
            self._logger.error(f"Failed to load cache for key '{key}': {e}")
            raise CacheManagerError(f"Error loading cache for key '{key}'", error=str(e))

    def save(self, key: str, data: Any):
        """Save data to the cache using the provided key.

        Args:
            key (str): The cache key to store data for.
            data (Any): The data to be cached.
        """
        try:
            self._logger.debug(f"Saving data to cache for key: {key}")
            self._backend.save(key, data)
        except Exception as e:
            self._logger.error(f"Failed to save cache for key '{key}': {e}")
            raise CacheManagerError(f"Error saving cache for key '{key}'", error=str(e))

    def invalidate(self, key: str):
        """Invalidate a specific cache entry.

        Args:
            key (str): The cache key to invalidate.
        """
        try:
            self._logger.debug(f"Invalidating cache for key: {key}")
            self._backend.invalidate(key)
        except Exception as e:
            self._logger.error(f"Failed to invalidate cache for key '{key}': {e}")
            raise CacheManagerError(f"Error invalidating cache for key '{key}'", error=str(e))

    def clear_all(self):
        """Clears all cache entries for the backend."""
        try:
            self._logger.debug("Clearing all cache entries")
            self._backend.clear_all()
        except Exception as e:
            self._logger.error(f"Failed to clear cache: {e}")
            raise CacheManagerError("Error clearing all cache entries", error=str(e))

    def generate_cache_key(self, prefix: str, **kwargs) -> str:
        """Generates a cache key based on a prefix and additional parameters.

        Args:
            prefix (str): The base prefix for the cache key.
            **kwargs: Additional parameters to include in the cache key.

        Returns:
            str: The generated cache key.
        """
        # Sort the items and convert them to a JSON string for deterministic ordering
        sorted_items = JSONManager.create_json(kwargs)
        # Create a consistent hash using SHA-256
        hash_object = hashlib.sha256(sorted_items.encode("utf-8"))
        # Convert the hash to a hexadecimal string
        hash_hex = hash_object.hexdigest()
        return f"{prefix}_{hash_hex}"
