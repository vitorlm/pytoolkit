import datetime
import logging
import os
from logging.handlers import TimedRotatingFileHandler

# Log file directory in the root of the project
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../../logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOG_DIR, "application.log")

# Configuração de tempo para limpar logs antigos (em horas)
LOG_RETENTION_HOURS = 1  # Este valor pode ser ajustado conforme necessário


def configure_logging():
    """Configure logging for the entire application."""
    # Create a logger
    logger = logging.getLogger()

    # Check if the logger already has handlers to avoid duplication
    if logger.hasHandlers():
        return

    logger.setLevel(logging.INFO)

    # Define log format
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    # File handler with time rotation
    file_handler = TimedRotatingFileHandler(
        LOG_FILE_PATH, when="h", interval=1, backupCount=0
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("Logging configuration completed.")

    # Perform log cleanup
    cleanup_old_logs()


def cleanup_old_logs():
    """Remove log files older than LOG_RETENTION_HOURS."""
    now = datetime.datetime.now()
    for filename in os.listdir(LOG_DIR):
        file_path = os.path.join(LOG_DIR, filename)
        if os.path.isfile(file_path) and filename.startswith("application"):
            file_creation_time = datetime.datetime.fromtimestamp(
                os.path.getctime(file_path)
            )
            elapsed_time = (
                now - file_creation_time
            ).total_seconds() / 3600  # Convert to hours
            if elapsed_time > LOG_RETENTION_HOURS:
                try:
                    os.remove(file_path)
                    logging.info(f"Old log file '{filename}' removed successfully.")
                except Exception as e:
                    logging.error(f"Failed to remove old log file '{filename}': {e}")
