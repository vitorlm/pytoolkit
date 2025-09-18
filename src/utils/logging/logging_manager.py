import logging
import os
from enum import Enum
from logging import Logger
from logging.handlers import TimedRotatingFileHandler
from typing import List, Optional

from utils.file_manager import FileManager


class LogLevel(Enum):
    """
    Enum for log levels to improve readability and usability.

    Attributes:
        DEBUG: Debug log level.
        INFO: Info log level.
        WARNING: Warning log level.
        ERROR: Error log level.
        CRITICAL: Critical log level.
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
            level (LogLevel): The logging level to filter.
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
        "\033[95m",
        "\033[96m",
        "\033[93m",
        "\033[92m",
        "\033[94m",
        "\033[90m",
        "\033[91m",
        "\033[97m",
        "\033[36m",
        "\033[35m",
        "\033[34m",
        "\033[33m",
        "\033[32m",
        "\033[31m",
    ]

    def __init__(self, logger_number: int):
        """
        Initializes the formatter with a specific color based on a unique logger number.

        Args:
            logger_number (int): Unique identifier for assigning a color.
        """
        super().__init__()
        self.color = self.MODULE_COLORS[logger_number % len(self.MODULE_COLORS)]

    def format(self, record: logging.LogRecord) -> str:
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
    A custom logging handler to intercept logs and apply custom formatting.
    """

    def __init__(self, formatter: Optional[logging.Formatter] = None):
        """
        Initializes the custom log handler.

        Args:
            formatter (logging.Formatter, optional): The formatter to use. Defaults to None.
        """
        super().__init__()
        self.formatter = formatter

    def setFormatter(self, fmt: Optional[logging.Formatter]):
        """
        Sets the formatter for the handler.

        Args:
            fmt (Optional[logging.Formatter]): The formatter to set.
        """
        self.formatter = fmt

    def emit(self, record: logging.LogRecord):
        """
        Emits a log record using the provided formatter.

        Args:
            record (logging.LogRecord): The log record to process.

        Raises:
            ValueError: If no formatter is set for the handler.
        """
        if not self.formatter:
            raise ValueError("No formatter set for the CustomLogHandler.")
        log_message = self.format(record)
        print(log_message)


class LogManager:
    """
    Singleton LogManager to manage loggers for a CLI project.
    """

    _instance = None

    @staticmethod
    def initialize(
        log_dir: str,
        log_file: str,
        log_retention_hours: int,
        default_level: LogLevel = LogLevel.INFO,
        use_filter: bool = False,
    ):
        """
        Initializes the singleton instance of LogManager.

        Args:
            log_dir (str): Directory where log files are saved.
            log_file (str): Name of the log file.
            log_retention_hours (int): Retention period for old logs in hours.
            default_level (LogLevel): Default logging level. Defaults to LogLevel.INFO.
            use_filter (bool): Whether to use level-based filtering. Defaults to False.
        """
        if LogManager._instance is None:
            LogManager._instance = LogManager(log_dir, log_file, log_retention_hours, default_level, use_filter)

    @staticmethod
    def get_instance():
        """
        Returns the singleton instance of LogManager.
        """
        if LogManager._instance is None:
            raise RuntimeError("LogManager is not initialized. Call `LogManager.initialize()` first.")
        return LogManager._instance

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
            log_dir (str): Directory where log files are saved.
            log_file (str): Name of the log file.
            log_retention_hours (int): Retention period for old logs in hours.
            default_level (LogLevel): Default logging level. Defaults to LogLevel.INFO.
            use_filter (bool): Whether to use level-based filtering. Defaults to False.

        Raises:
            TypeError: If the arguments have invalid types.
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

        FileManager.create_folder(self.log_dir)
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
        logger.setLevel(self.default_level.value)

        # Check if the logger already has handlers to avoid duplication
        if logger.hasHandlers():
            return

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(ColorFormatter(logger_number=module_index))
        logger.addHandler(console_handler)

        # File handler
        log_file_path = os.path.join(self.log_dir, self.log_file)
        file_handler = TimedRotatingFileHandler(log_file_path, when="h", interval=1, backupCount=0)
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
        Retrieves or creates a logger instance.

        Args:
            name (Optional[str]): The base name of the logger. Defaults to None, which uses the
                                  main logger name.
            module_name (Optional[str]): The name of the module for the logger. If provided,
                                         the logger will include the module name.

        Returns:
            Logger: The configured logger instance.
        """
        logger_name = name if isinstance(name, str) and name.strip() else self.main_name

        if module_name:
            logger_name = f"{logger_name}.{module_name}"

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

    @staticmethod
    def add_custom_handler(
        logger_name: str,
        formatter: Optional[logging.Formatter] = None,
        replace_existing: bool = False,
        disable_propagation: bool = True,
        handler_id: Optional[str] = None,
    ) -> None:
        """
        Adds a custom logging handler to a specific logger.

        Args:
            logger_name (str): The name of the logger to add the handler to.
            formatter (Optional[logging.Formatter]): Formatter to apply to the custom handler.
                                                     Defaults to None.
            replace_existing (bool): If True, removes existing handlers before adding the new one.
                                     Defaults to False.
            disable_propagation (bool): If True, disables propagation to the root logger.
                                        Defaults to True.
            handler_id (Optional[str]): Unique identifier for the handler,
                                        for managing multiple handlers. Defaults to None.

        Raises:
            ValueError: If the logger cannot be retrieved or created.
        """
        try:
            log_manager = LogManager.get_instance()
            logger = log_manager.get_logger(logger_name)
            if not logger:
                raise ValueError(f"Logger with name '{logger_name}' could not be found or created.")

            if replace_existing:
                while logger.handlers:
                    logger.removeHandler(logger.handlers[0])

            if not formatter:
                formatter = ColorFormatter(logger_number=99)

            handler = CustomLogHandler(formatter=formatter)

            # Store the handler with a unique identifier
            if handler_id:
                log_manager.custom_handlers[handler_id] = handler

            logger.setLevel(log_manager.default_level.value)
            logger.addHandler(handler)

            if disable_propagation:
                logger.propagate = False

        except Exception as e:
            raise RuntimeError(f"Failed to add custom handler to logger '{logger_name}': {e}") from e

    def list_log_files(self, extension: str = ".log") -> List[str]:
        """
        Lists all log files in the configured log directory.

        Args:
            extension (str): Extension of the files to be listed. Defaults to ".log".

        Returns:
            List[str]: A list of paths to the log files.
        """
        return FileManager.list_files(self.log_dir, extension)

    def read_log_file(self, file_name: str) -> List[str]:
        """
        Reads the content of a log file.

        Args:
            file_name (str): Name of the log file.

        Returns:
            List[str]: Content of the file as a list of lines.
        """
        file_path = os.path.join(self.log_dir, file_name)
        return FileManager.read_file(file_path)

    def delete_log_file(self, file_name: str) -> None:
        """
        Deletes a log file.

        Args:
            file_name (str): Name of the log file.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        file_path = os.path.join(self.log_dir, file_name)
        FileManager.delete_file(file_path)
