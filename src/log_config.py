import logging

from config import Config
from utils.logging_manager import CustomLogHandler, LogLevel, LogManager

# Map log levels from .env to LogLevel Enum
LOG_LEVEL_MAP = {
    "DEBUG": LogLevel.DEBUG,
    "INFO": LogLevel.INFO,
    "WARNING": LogLevel.WARNING,
    "ERROR": LogLevel.ERROR,
    "CRITICAL": LogLevel.CRITICAL,
}

# Instantiate the LogManager singleton using environment variables
log_manager = LogManager(
    log_dir=Config.LOG_DIR,
    log_file=Config.LOG_FILE,
    log_retention_hours=Config.LOG_RETENTION_HOURS,
    default_level=LOG_LEVEL_MAP.get(Config.LOG_LEVEL, LogLevel.INFO).value,
    use_filter=Config.USE_FILTER == "true",
)

custom_handler = CustomLogHandler()
custom_handler.setFormatter(
    logging.Formatter(
        "[%(asctime)s][%(levelname)s][%(name)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
)
