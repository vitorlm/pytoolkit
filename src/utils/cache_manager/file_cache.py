from datetime import datetime
import os
from typing import Any, Optional
from utils.file_manager import FileManager
from utils.data.json_manager import JSONManager
from log_config import log_manager
from utils.cache_manager.cache_backend import CacheBackend
from utils.cache_manager.error import FileCacheError


class FileCacheBackend(CacheBackend):
    """
    File-based caching backend for managing cached data as JSON files.

    Args:
        cache_dir (str): Directory where cached files are stored.
    """

    _logger = log_manager.get_logger("FileCacheBackend")

    def __init__(self, cache_dir: str):
        try:
            self.cache_dir = cache_dir
            FileManager.create_folder(cache_dir)
        except Exception as e:
            raise FileCacheError(
                f"Failed to initialize cache directory: {cache_dir}",
                cache_dir=cache_dir,
                error=str(e),
            )

    def _get_file_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def load(self, key: str, expiration_minutes: Optional[int] = None) -> Optional[Any]:
        file_path = self._get_file_path(key)
        try:
            if not FileManager.file_exists(file_path):
                return None

            cache_data = JSONManager.read_json(file_path, default={})
            if expiration_minutes:
                cached_time = datetime.fromisoformat(cache_data["_cached_at"])
                if (datetime.now() - cached_time).total_seconds() > expiration_minutes * 60:
                    self.invalidate(key)
                    return None

            return cache_data.get("data")
        except FileNotFoundError:
            return None
        except Exception as e:
            raise FileCacheError(
                f"Failed to load cache for key '{key}'",
                key=key,
                expiration_minutes=expiration_minutes,
                error=str(e),
            )

    def save(self, key: str, data: Any):
        file_path = self._get_file_path(key)
        try:
            cache_data = {"data": data, "_cached_at": datetime.now().isoformat()}
            JSONManager.write_json(cache_data, file_path)
        except Exception as e:
            raise FileCacheError(
                f"Failed to save cache for key '{key}'",
                key=key,
                data=data,
                error=str(e),
            )

    def invalidate(self, key: str):
        file_path = self._get_file_path(key)
        try:
            FileManager.delete_file(file_path)
        except FileNotFoundError:
            self._logger.warning(f"Cache key '{key}' not found for invalidation.")
        except Exception as e:
            raise FileCacheError(
                f"Failed to invalidate cache for key '{key}'",
                key=key,
                error=str(e),
            )
