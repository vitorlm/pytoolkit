from datetime import datetime
import os
from typing import Any, Optional
from src.log_config import log_manager
from utils.file_manager import FileManager
from utils.json_manager import JSONManager
from utils.error_manager import handle_generic_exception, CacheError


class CacheManager:
    """
    A flexible CacheManager for managing cache data. Initially supports file-based caching
    but can be extended to other backends like Redis in the future.
    """

    _instance = None
    _logger = log_manager.get_logger("CacheManager")

    @staticmethod
    def initialize(cache_backend: Optional[str] = "file", cache_dir: Optional[str] = None):
        """
        Initializes the singleton instance of CacheManager.

        :param cache_backend: Backend type for caching (default is "file").
        :param cache_dir: Directory for file-based caching (ignored if backend is not "file").
        """
        if CacheManager._instance is None:
            CacheManager._instance = CacheManager(cache_backend, cache_dir)

    @staticmethod
    def get_instance():
        """
        Returns the singleton instance of CacheManager.

        :return: CacheManager instance.
        """
        if CacheManager._instance is None:
            raise RuntimeError(
                "CacheManager is not initialized. Call `CacheManager.initialize()` first."
            )
        return CacheManager._instance

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CacheManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, cache_backend: Optional[str] = "file", cache_dir: Optional[str] = None):
        """
        Initialize the CacheManager with a specified backend.

        :param cache_backend: Backend type for caching (default is "file").
        :param cache_dir: Directory for file-based caching (ignored if backend is not "file").
        """
        if hasattr(self, "_initialized") and self._initialized:
            return

        self.cache_backend = cache_backend

        if self.cache_backend == "file":
            self.cache_dir = cache_dir or os.path.join(os.path.dirname(__file__), "../../cache")
            try:
                FileManager.create_folder(self.cache_dir)
            except Exception as e:
                handle_generic_exception(e, f"Failed to create cache directory: {self.cache_dir}")
        else:
            self.cache_dir = None
            self._logger.info(f"Using backend: {self.cache_backend}")

        self._initialized = True

    def _get_file_path(self, key: str) -> str:
        """
        Generate the full file path for a given cache key.

        :param key: The cache key.
        :return: Full file path.
        """
        if self.cache_backend != "file" or not self.cache_dir:
            raise ValueError("File path generation is only valid for file-based caching.")
        return os.path.join(self.cache_dir, f"{key}.json")

    def load(self, key: str, expiration_minutes: Optional[int] = None) -> Optional[Any]:
        """
        Load data from the cache using the provided key.

        :param key: The cache key to retrieve data for.
        :param expiration_minutes: Expiration time in minutes. If None, cache is always valid.
        :return: The cached data if valid, otherwise None.
        """
        if self.cache_backend == "file":
            file_path = self._get_file_path(key)
            if not FileManager.validate_file(file_path, allowed_extensions=[".json"]):
                self._logger.info(f"Cache key '{key}' does not exist.")
                return None

            try:
                cache_data = JSONManager.read_json(file_path)
                if expiration_minutes is not None:
                    cached_time_str = cache_data.get("_cached_at")
                    if cached_time_str:
                        cached_time = datetime.fromisoformat(cached_time_str)
                        if (datetime.now() - cached_time).total_seconds() > expiration_minutes * 60:
                            self._logger.info(f"Cache key '{key}' has expired. Invalidating.")
                            self.invalidate(key)
                            return None

                self._logger.info(f"Loaded data from cache key: {key}")
                return cache_data.get("data")

            except Exception as e:
                handle_generic_exception(e, f"Error loading cache key '{key}'")
                return None

        else:
            self._logger.error("Unsupported backend for loading cache.")
            return None

    def save(self, key: str, data: Any):
        """
        Save data to the cache using the provided key.

        :param key: The cache key to store data for.
        :param data: The data to be cached.
        """
        if self.cache_backend == "file":
            file_path = self._get_file_path(key)
            cache_data = {
                "data": data,
                "_cached_at": datetime.now().isoformat(),
            }

            try:
                JSONManager.write_json(cache_data, file_path)
                self._logger.info(f"Data successfully saved to cache key: {key}")

            except Exception as e:
                handle_generic_exception(e, f"Failed to save cache key '{key}'")

        else:
            self._logger.error("Unsupported backend for saving cache.")
            raise CacheError("Unsupported cache backend.")

    def invalidate(self, key: str):
        """
        Invalidate a specific cache entry.

        :param key: The cache key to invalidate.
        """
        if self.cache_backend == "file":
            file_path = self._get_file_path(key)
            try:
                FileManager.delete_file(file_path)
                self._logger.info(f"Cache key '{key}' invalidated successfully.")

            except FileNotFoundError:
                self._logger.warning(f"Cache key '{key}' does not exist to invalidate.")
            except Exception as e:
                handle_generic_exception(e, f"Failed to invalidate cache key '{key}'")

        else:
            self._logger.error("Unsupported backend for invalidating cache.")
            raise CacheError("Unsupported cache backend.")
