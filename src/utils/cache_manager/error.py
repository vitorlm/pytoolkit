from utils.error.base_custom_error import BaseCustomError


class CacheManagerError(BaseCustomError):
    """
    Base exception for cache manager-related errors.
    """


class FileCacheError(CacheManagerError):
    """
    Exception for file-based caching errors.
    """
