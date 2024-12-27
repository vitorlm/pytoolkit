import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """
    Configuration class for loading environment variables with validation.
    """

    LOG_DIR = os.getenv("LOG_DIR")
    LOG_FILE = os.getenv("LOG_FILE")

    LOG_LEVEL = os.getenv("LOG_LEVEL")
    if LOG_LEVEL not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError(
            f"Invalid LOG_LEVEL: {LOG_LEVEL}. Must be one of 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'."
        )

    LOG_OUTPUT = os.getenv("LOG_OUTPUT")
    if LOG_OUTPUT and LOG_OUTPUT.lower() not in {"console", "file", "both"}:
        raise ValueError(
            f"Invalid LOG_OUTPUT: {LOG_OUTPUT}. Must be one of 'console', 'file', or 'both'."
        )

    LOG_RETENTION_HOURS = os.getenv("LOG_RETENTION_HOURS")
    if LOG_RETENTION_HOURS:
        try:
            LOG_RETENTION_HOURS = int(LOG_RETENTION_HOURS)
        except ValueError:
            raise ValueError("LOG_RETENTION_HOURS must be an integer.")

    USE_FILTER = os.getenv("USE_FILTER")
    if USE_FILTER and USE_FILTER.lower() not in {"true", "false"}:
        raise ValueError(f"Invalid USE_FILTER: {USE_FILTER}. Must be 'true' or 'false'.")
