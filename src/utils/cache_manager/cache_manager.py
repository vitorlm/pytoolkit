import os
from typing import Any, Optional
from src.log_config import log_manager
from utils.cache_manager.error import CacheManagerError


class CacheManager:
    """
    A flexible CacheManager for managing cache data with a singleton pattern.
    Supports file-based caching and is extensible for other backends (e.g., Redis).
    """

    _instance = None  # Singleton instance
    _logger = log_manager.get_logger("CacheManager")

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CacheManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, cache_backend: str = "file", cache_dir: Optional[str] = None):
        """
        Initializes the CacheManager. This follows a singleton pattern.

        Args:
            cache_backend (str): Backend type for caching (default: "file").
            cache_dir (Optional[str]): Directory for file-based caching.

        Raises:
            CacheManagerError: If the backend is unsupported.
        """
        if hasattr(self, "_initialized") and self._initialized:
            return  # Avoid reinitialization

        self.cache_backend = cache_backend
        self._logger.info(f"Initializing CacheManager with backend: {cache_backend}")

        # Dynamically initialize the backend
        self._backend = self._initialize_backend(cache_backend, cache_dir)

        self._initialized = True

    def _initialize_backend(self, cache_backend: str, cache_dir: Optional[str] = None):
        """
        Initializes the specified cache backend.

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
                cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), "../../cache")
                raise CacheManagerError(
                    f"Unsupported cache backend: {cache_backend}", backend=cache_backend
                )
        except Exception as e:
            raise CacheManagerError(
                f"Failed to initialize backend '{cache_backend}'",
                backend=cache_backend,
                cache_dir=cache_dir,
                error=str(e),
            )

    def load(self, key: str, expiration_minutes: Optional[int] = None) -> Optional[Any]:
        """
        Load data from the cache using the provided key.

        Args:
            key (str): The cache key to retrieve data for.
            expiration_minutes (Optional[int]): Expiration time in minutes.

        Returns:
            Optional[Any]: The cached data if valid, otherwise None.
        """
        try:
            return self._backend.load(key, expiration_minutes)
        except Exception as e:
            self._logger.error(f"Failed to load cache for key '{key}': {e}")
            raise

    def save(self, key: str, data: Any):
        """
        Save data to the cache using the provided key.

        Args:
            key (str): The cache key to store data for.
            data (Any): The data to be cached.
        """
        try:
            self._backend.save(key, data)
        except Exception as e:
            self._logger.error(f"Failed to save cache for key '{key}': {e}")
            raise

    def invalidate(self, key: str):
        """
        Invalidate a specific cache entry.

        Args:
            key (str): The cache key to invalidate.
        """
        try:
            self._backend.invalidate(key)
        except Exception as e:
            self._logger.error(f"Failed to invalidate cache for key '{key}': {e}")
            raise
