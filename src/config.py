import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """
    Configuration class for loading environment variables with validation.
    """

    # Logging settings
    LOG_DIR = os.getenv("LOG_DIR", "./logs")
    LOG_FILE = os.getenv("LOG_FILE", "pytoolkit.log")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    if LOG_LEVEL not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError(
            f"Invalid LOG_LEVEL: {LOG_LEVEL}. Must be one of 'DEBUG', 'INFO', "
            "'WARNING', 'ERROR', 'CRITICAL'."
        )
    LOG_OUTPUT = os.getenv("LOG_OUTPUT", "both")
    if LOG_OUTPUT.lower() not in {"console", "file", "both"}:
        raise ValueError(
            f"Invalid LOG_OUTPUT: {LOG_OUTPUT}. Must be one of 'console', 'file', or 'both'."
        )

    LOG_RETENTION_HOURS = int(os.getenv("LOG_RETENTION_HOURS", "24"))

    # Cache settings
    CACHE_BACKEND = os.getenv("CACHE_BACKEND", "file")
    CACHE_DIR = os.getenv("CACHE_DIR", "./cache")

    # Additional settings
    USE_FILTER = os.getenv("USE_FILTER", "false").lower()
    if USE_FILTER not in {"true", "false"}:
        raise ValueError(
            f"Invalid USE_FILTER: {USE_FILTER}. Must be 'true' or 'false'."
        )
