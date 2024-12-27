import os

from dotenv import load_dotenv

load_dotenv(f"{os.path.dirname(os.path.abspath(__file__))}/.env")


class Config:
    HOST = os.getenv("OLLAMA_HOST")
    MODEL = os.getenv("OLLAMA_MODEL")
    NUM_CTX = int(os.getenv("OLLAMA_NUM_CTX")) if os.getenv("OLLAMA_NUM_CTX") else None
    TEMPERATURE = (
        float(os.getenv("OLLAMA_TEMPERATURE")) if os.getenv("OLLAMA_TEMPERATURE") else None
    )
    NUM_THREAD = int(os.getenv("OLLAMA_NUM_THREAD")) if os.getenv("OLLAMA_NUM_THREAD") else None
    NUM_KEEP = int(os.getenv("OLLAMA_NUM_KEEP")) if os.getenv("OLLAMA_NUM_KEEP") else None
    TOP_K = int(os.getenv("OLLAMA_TOP_K")) if os.getenv("OLLAMA_TOP_K") else None
    TOP_P = float(os.getenv("OLLAMA_TOP_P")) if os.getenv("OLLAMA_TOP_P") else None
    REPEAT_PENALTY = (
        float(os.getenv("OLLAMA_REPEAT_PENALTY")) if os.getenv("OLLAMA_REPEAT_PENALTY") else None
    )
    STOP = os.getenv("OLLAMA_STOP").split(",") if os.getenv("OLLAMA_STOP") else None
    NUM_PREDICT = int(os.getenv("OLLAMA_NUM_PREDICT")) if os.getenv("OLLAMA_NUM_PREDICT") else None
    PRESENCE_PENALTY = (
        float(os.getenv("OLLAMA_PRESENCE_PENALTY"))
        if os.getenv("OLLAMA_PRESENCE_PENALTY")
        else None
    )
    FREQUENCY_PENALTY = (
        float(os.getenv("OLLAMA_FREQUENCY_PENALTY"))
        if os.getenv("OLLAMA_FREQUENCY_PENALTY")
        else None
    )
    COL_MEMBERS = os.getenv("COL_MEMBERS", "I")
    ROW_MEMBERS_START = int(os.getenv("ROW_MEMBERS_START"))
    ROW_MEMBERS_END = int(os.getenv("ROW_MEMBERS_END"))
    ROW_DAYS = int(os.getenv("ROW_DAYS"))
    TASKS_TO_IGNORE = [task.strip() for task in os.getenv("TASKS_TO_IGNORE", "").split(",")]
    COL_TASKS_START = os.getenv("COL_TASKS_START")
    COL_TASKS_END = os.getenv("COL_TASKS_END")
    COL_TASKS_ASSIGNMENT_START = os.getenv("COL_TASKS_ASSIGNMENT_START")
    ROW_TASKS_ASSIGNMENT_START = int(os.getenv("ROW_TASKS_ASSIGNMENT_START"))
    ROW_TASKS_ASSIGNMENT_END = int(os.getenv("ROW_TASKS_ASSIGNMENT_END"))
    ROW_HEADER_START = int(os.getenv("ROW_HEADER_START"))
    ROW_HEADER_END = int(os.getenv("ROW_HEADER_END"))
    CRITERIA_WEIGHTS = eval(os.getenv("CRITERIA_WEIGHTS"))
    OUTLIER_THRESHOLD = float(os.getenv("OUTLIER_THRESHOLD"))
    PERCENTILE_Q1 = float(os.getenv("PERCENTILE_Q1"))
    PERCENTILE_Q3 = float(os.getenv("PERCENTILE_Q3"))
