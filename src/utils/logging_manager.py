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
    A logging formatter that applies colors to log messages based on level and a unique identifier.
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
        "\033[90m",  # Gray
        "\033[91m",  # Red
        "\033[97m",  # White
        "\033[36m",  # Bright Cyan
        "\033[35m",  # Bright Purple
        "\033[34m",  # Bright Blue
        "\033[33m",  # Bright Yellow
        "\033[32m",  # Bright Green
        "\033[31m",  # Bright Red
    ]

    def __init__(self, logger_number: int):
        """
        Initializes the formatter with a specific color based on a unique logger number.

        Args:
            logger_number (int): Unique identifier for assigning a color.
        """
        super().__init__()
        self.color = self.MODULE_COLORS[logger_number % len(self.MODULE_COLORS)]

    def format(self, record):
        """
        Formats the log record with level-based and unique color.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: The formatted log message with color codes.
        """
        log_fmt = (
            self.LEVEL_COLORS.get(record.levelno, self.LEVEL_COLORS["RESET"])
            + "[%(asctime)s]"
            + self.LEVEL_COLORS.get(record.levelname, self.LEVEL_COLORS["RESET"])
            + "[%(levelname)s]"
            + self.LEVEL_COLORS["RESET"]
            + self.color
            + "[%(name)s]"
            + self.LEVEL_COLORS["RESET"]
            + ": %(message)s"
        )
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class CustomLogHandler(logging.Handler):
    """
    A custom logging handler to intercept logs from specific libraries and apply a custom format.
    """

    def __init__(self, formatter: Optional[logging.Formatter] = None):
        """
        Initializes the custom log handler.

        Args:
            formatter (logging.Formatter, optional): The formatter to use. Defaults to None.
        """
        super().__init__()
        self.formatter = formatter

    def setFormatter(self, formatter: logging.Formatter):
        """
        Sets the formatter for the handler.

        Args:
            formatter (logging.Formatter): The formatter to set.
        """
        self.formatter = formatter

    def emit(self, record: logging.LogRecord):
        """
        Emits a log record using the provided formatter.

        Args:
            record (logging.LogRecord): The log record to process.
        """
        if self.formatter is None:
            raise ValueError("No formatter set for the CustomLogHandler.")
        log_message = self.format(record)
        print(log_message)


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
        self.custom_handlers = {}

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
        console_handler.setFormatter(ColorFormatter(logger_number=module_index))
        logger.addHandler(console_handler)

        # File handler
        log_file_path = os.path.join(self.log_dir, self.log_file)
        file_handler = TimedRotatingFileHandler(
            log_file_path, when="h", interval=1, backupCount=0
        )
        file_handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s][%(levelname)s][%(name)s]: %(message)s",
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

    def get_logger(
        self,
        name: Optional[str] = None,
        module_name: Optional[str] = None,
    ) -> Logger:
        """
        Retrieves or creates a logger instance. If a logger with the specified name already exists,
        it reuses it; otherwise, it creates a new one, including support for module-specific logging.

        Args:
            name (str, optional): The base name of the logger. Defaults to None, which uses the main logger name.
            module_name (str, optional): The name of the module for the logger. If provided, the logger will
            include the module name.

        Returns:
            Logger: The configured logger instance.
        """
        # Determine the logger's base name
        logger_name = name if isinstance(name, str) and name.strip() else self.main_name

        # Append module name if provided
        if module_name:
            logger_name = f"{logger_name}.{module_name}"

        # Retrieve or create the logger
        if logger_name in self.loggers:
            return self.loggers[logger_name]
        elif logger_name in logging.Logger.manager.loggerDict:
            logger = logging.getLogger(logger_name)
            self.loggers[logger_name] = logger
        else:
            module_index = len(self.loggers)
            self._initialize_logger(logger_name, module_index)
            logger = self.loggers[logger_name]

        return logger

    def set_logger_level(self, module_name: Optional[str], level: int):
        """
        Sets the logging level for a specific logger.

        Args:
            module_name (str, optional): The name of the module. Defaults to None for the main logger.
            level (int): The logging level to set.
        """
        logger = self.get_logger(module_name=module_name)
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
        logger = self.get_logger(module_name=module_name)
        for handler in logger.handlers:
            handler.addFilter(LevelFilter(level))

    def disable_level_filter(self, module_name: Optional[str]):
        """
        Disables level filtering for a specific logger.

        Args:
            module_name (str, optional): The name of the module. Defaults to None for the main logger.
        """
        logger = self.get_logger(module_name=module_name)
        for handler in logger.handlers:
            handler.filters.clear()

    def add_custom_handler(
        self,
        logger_name: str,
        replace_existing: bool = False,
        disable_propagation: bool = True,
    ):
        """
        Adds a custom logging handler to a specific logger. Optionally replaces existing handlers and disables propagation.

        Args:
            logger_name (str): The name of the logger to add the handler to.
            module_index (int): Index for assigning module colors for the formatter.
            replace_existing (bool, optional): If True, removes existing handlers before adding the new one.
            disable_propagation (bool, optional): If True, disables propagation to the root logger.

        Raises:
            ValueError: If the logger cannot be retrieved or created.
            RuntimeError: If adding the handler fails.
        """
        try:
            # Retrieve or create the logger
            logger = self.get_logger(name=logger_name)
            if logger is None:
                raise ValueError(
                    f"Logger with name '{logger_name}' could not be found or created."
                )

            # Replace existing handlers if the flag is set
            if replace_existing:
                while logger.handlers:
                    logger.removeHandler(logger.handlers[0])

            # Create and configure the custom handler with ColorFormatter
            formatter = ColorFormatter(logger_number=99)
            handler = CustomLogHandler(formatter=formatter)

            # Add the custom handler to the logger
            self.custom_handlers[logger_name] = handler
            logger.setLevel(self.default_level)
            logger.addHandler(handler)

            # Disable propagation if required
            if disable_propagation:
                logger.propagate = False

        except Exception as e:
            raise RuntimeError(
                f"Failed to add custom handler to logger '{logger_name}': {e}"
            ) from e

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
