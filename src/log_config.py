import logging

from config import Config
from utils.logging.logging_manager import LogLevel, LogManager

# Map log levels from .env to LogLevel Enum
LOG_LEVEL_MAP = {
    "DEBUG": LogLevel.DEBUG,
    "INFO": LogLevel.INFO,
    "WARNING": LogLevel.WARNING,
    "ERROR": LogLevel.ERROR,
    "CRITICAL": LogLevel.CRITICAL,
}

if Config.LOG_LEVEL not in LOG_LEVEL_MAP:
    raise ValueError(
        f"Invalid LOG_LEVEL: {Config.LOG_LEVEL}. Must be one of 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'."
    )

# Instantiate the LogManager singleton using validated environment variables
log_manager = LogManager(
    log_dir=Config.LOG_DIR,
    log_file=Config.LOG_FILE,
    log_retention_hours=Config.LOG_RETENTION_HOURS,
    default_level=LOG_LEVEL_MAP.get(Config.LOG_LEVEL, LogLevel.INFO),
    use_filter=Config.USE_FILTER == "true",
    log_output=Config.LOG_OUTPUT,
)

# Custom handler with validated configurations
custom_handler = logging.StreamHandler()
custom_handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
