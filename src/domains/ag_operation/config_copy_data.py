import os

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


class Config:
    SOURCE_CONFIG = {
        "aws_access_key_id": os.getenv("SOURCE_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("SOURCE_AWS_SECRET_ACCESS_KEY"),
        "region_name": os.getenv("SOURCE_AWS_REGION"),
    }

    TARGET_CONFIG = {
        "aws_access_key_id": os.getenv("TARGET_AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("TARGET_AWS_SECRET_ACCESS_KEY"),
        "region_name": os.getenv("TARGET_AWS_REGION"),
    }

    AGRO_OPERATIONS_TABLE = os.getenv("AGRO_OPERATIONS_TABLE")
    REVERSED_KEYS_TABLE = os.getenv("REVERSED_KEYS_TABLE")
    PRODUCT_TYPE_TABLE = os.getenv("PRODUCT_TYPE_TABLE")
    SUMMARIZED_AGRO_OP_TABLE = os.getenv("SUMMARIZED_AGRO_OP_TABLE")
    SUMMARIZED_AGRO_OP_TABLE = os.getenv("SUMMARIZED_AGRO_OP_TABLE")
