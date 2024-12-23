import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    LOG_DIR = os.getenv("LOG_DIR")
    LOG_FILE = os.getenv("LOG_FILE")
    LOG_LEVEL = os.getenv("LOG_LEVEL").upper() if os.getenv("LOG_LEVEL") else None
    LOG_OUTPUT = os.getenv("LOG_OUTPUT").lower() if os.getenv("LOG_OUTPUT") else None
    LOG_RETENTION_HOURS = (
        int(os.getenv("LOG_RETENTION_HOURS"))
        if os.getenv("LOG_RETENTION_HOURS")
        else None
    )
    USE_FILTER = os.getenv("USE_FILTER").lower() if os.getenv("USE_FILTER") else False
