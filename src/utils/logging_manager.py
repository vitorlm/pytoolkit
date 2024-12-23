import datetime
import logging
import os
from enum import Enum
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
from typing import Optional


class LogLevel(Enum):
    """
    Enum for log levels to improve readability and usability.
    """

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class LevelFilter(logging.Filter):
    """
    A logging filter that allows filtering log messages by their level.
    """

    def __init__(self, level: LogLevel):
        """
        Initializes the filter with a specific logging level.

        Args:
            level (int): The logging level to filter.
        """
        self.level = level

    def filter(self, record: logging.LogRecord) -> bool:
        """
        Filters log records based on their level.

        Args:
            record (logging.LogRecord): Log record to evaluate.

        Returns:
            bool: True if the record matches the level, False otherwise.
        """
        return record.levelno == self.level


class ColorFormatter(logging.Formatter):
    """
    A logging formatter that applies colors to log messages based on level and module name.
    """

    LEVEL_COLORS = {
        LogLevel.DEBUG: "\033[94m",  # Blue
        LogLevel.INFO: "\033[92m",  # Green
        LogLevel.WARNING: "\033[93m",  # Yellow
        LogLevel.ERROR: "\033[91m",  # Red
        LogLevel.CRITICAL: "\033[91m\033[1m",  # Bold Red
        "RESET": "\033[0m",
    }

    MODULE_COLORS = [
        "\033[95m",  # Purple
        "\033[96m",  # Cyan
        "\033[93m",  # Yellow
        "\033[92m",  # Green
        "\033[94m",  # Blue
    ]

    # Map log levels from .env to LogLevel Enum
    LOG_LEVEL_MAP = {
        "DEBUG": LogLevel.DEBUG,
        "INFO": LogLevel.INFO,
        "WARNING": LogLevel.WARNING,
        "ERROR": LogLevel.ERROR,
        "CRITICAL": LogLevel.CRITICAL,
    }

    def __init__(self, module_index: int = 0):
        """
        Initializes the formatter with a specific module color.

        Args:
            module_index (int): Index to determine the module color from MODULE_COLORS.
        """
        super().__init__()
        self.module_color = self.MODULE_COLORS[module_index % len(self.MODULE_COLORS)]

    def format(self, record):
        """
        Formats the log record with level-based and module-based colors.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: The formatted log message with color codes.
        """
        log_fmt = (
            self.LEVEL_COLORS.get(record.levelno, self.LEVEL_COLORS["RESET"])
            + "[%(asctime)s]"
            + self.LEVEL_COLORS[self.LOG_LEVEL_MAP.get(record.levelname)]
            + "[%(levelname)s]"
            + self.LEVEL_COLORS["RESET"]
            + self.module_color
            + f"[{record.name}]"
            + self.LEVEL_COLORS["RESET"]
            + ": %(message)s"
        )
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class LogManager:
    """
    Singleton LogManager to manage loggers for a CLI project.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(LogManager, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        log_dir: str,
        log_file: str,
        log_retention_hours: int,
        default_level: LogLevel = LogLevel.INFO,
        use_filter: bool = False,
    ):
        """
        Initializes the LogManager.

        Args:
            main_name (str): The name of the main logger.
            log_dir (str): Directory where log files are saved.
            log_file (str): Name of the log file.
            log_retention_hours (int): Retention period for old logs in hours.
            default_level (int): Default logging level.
            use_filter (bool): Whether to use level-based filtering.
        """
        if hasattr(self, "_initialized") and self._initialized:
            return

        if not isinstance(log_dir, str):
            raise TypeError("log_dir must be a string")
        if not isinstance(log_file, str):
            raise TypeError("log_file must be a string")
        if not isinstance(log_retention_hours, int):
            raise TypeError("log_retention_hours must be an integer")

        self.main_name = "__main__"
        self.log_dir = log_dir
        self.log_file = log_file
        self.log_retention_hours = log_retention_hours
        self.default_level = default_level
        self.use_filter = use_filter
        self.loggers = {}

        os.makedirs(self.log_dir, exist_ok=True)
        self._initialize_logger(self.main_name)
        self._initialized = True

    def _initialize_logger(self, name: str, module_index: int = 0):
        """
        Configures a logger with console and file handlers.

        Args:
            name (str): The name of the logger.
            module_index (int): Index for assigning module colors.
        """
        logger = logging.getLogger(name)

        # Disable propagation to prevent handler inheritance
        logger.propagate = False

        # Set the logger level
        logger.setLevel(self.default_level)

        # Check if the logger already has handlers to avoid duplication
        if logger.hasHandlers():
            return

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ColorFormatter(module_index))
        logger.addHandler(console_handler)

        # File handler
        log_file_path = os.path.join(self.log_dir, self.log_file)
        file_handler = TimedRotatingFileHandler(
            log_file_path, when="h", interval=1, backupCount=0
        )
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)

        # Add level filter if enabled
        if self.use_filter:
            level_filter = LevelFilter(self.default_level)
            console_handler.addFilter(level_filter)
            file_handler.addFilter(level_filter)

        self.loggers[name] = logger

    def get_logger(self, module_name: Optional[str] = None) -> Logger:
        """
        Retrieves or creates a logger for the main or module-specific purpose.

        Args:
            module_name (str, optional): The name of the module. Defaults to None.

        Returns:
            Logger: The configured logger instance.
        """
        if module_name is None or module_name == self.main_name:
            name = self.main_name
        else:
            name = f"{self.main_name}.{module_name}"

        if name in self.loggers:
            return self.loggers[name]

        module_index = len(self.loggers)
        self._initialize_logger(name, module_index)
        return self.loggers[name]

    def set_logger_level(self, module_name: Optional[str], level: int):
        """
        Sets the logging level for a specific logger.

        Args:
            module_name (str, optional): The name of the module. Defaults to None for the main logger.
            level (int): The logging level to set.
        """
        logger = self.get_logger(module_name)
        logger.setLevel(level)

        for handler in logger.handlers:
            handler.setLevel(level)

    def enable_level_filter(self, module_name: Optional[str], level: int):
        """
        Enables a level filter for a specific logger.

        Args:
            module_name (str, optional): The name of the module. Defaults to None for the main logger.
            level (int): The logging level to filter.
        """
        logger = self.get_logger(module_name)
        for handler in logger.handlers:
            handler.addFilter(LevelFilter(level))

    def disable_level_filter(self, module_name: Optional[str]):
        """
        Disables level filtering for a specific logger.

        Args:
            module_name (str, optional): The name of the module. Defaults to None for the main logger.
        """
        logger = self.get_logger(module_name)
        for handler in logger.handlers:
            handler.filters.clear()

    def cleanup_old_logs(self):
        """
        Removes old log files based on the configured retention period.
        """
        now = datetime.datetime.now()
        for filename in os.listdir(self.log_dir):
            file_path = os.path.join(self.log_dir, filename)
            if os.path.isfile(file_path) and filename.startswith(
                self.log_file.split(".")[0]
            ):
                file_creation_time = datetime.datetime.fromtimestamp(
                    os.path.getctime(file_path)
                )
                elapsed_time = (now - file_creation_time).total_seconds() / 3600
                if elapsed_time > self.log_retention_hours:
                    try:
                        os.remove(file_path)
                        self.loggers[self.main_name].info(
                            f"Old log file '{filename}' removed."
                        )
                    except OSError as e:
                        self.loggers[self.main_name].warning(
                            f"Failed to remove old log file '{filename}': {e}"
                        )
