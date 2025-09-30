import os
from dotenv import load_dotenv

# Load environment variables from the catalog domain .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


class CatalogConfig:
    """Configuration class for Catalog domain operations."""

    # Cropwise API Configuration
    CROPWISE_API_BASE_URL = os.getenv(
        "CROPWISE_API_BASE_URL", "https://api.cropwise.com"
    )
    CROPWISE_API_KEY = os.getenv("CROPWISE_API_KEY")

    # Default values for operations
    DEFAULT_SOURCE = os.getenv("DEFAULT_SOURCE", "TUBE")
    DEFAULT_COUNTRY = os.getenv("DEFAULT_COUNTRY", "BR")
    DEFAULT_BATCH_SIZE = int(os.getenv("DEFAULT_BATCH_SIZE", "1000"))
    DEFAULT_CACHE_DURATION = int(os.getenv("DEFAULT_CACHE_DURATION", "60"))

    # Optional organization configuration
    DEFAULT_ORG_ID = os.getenv("DEFAULT_ORG_ID")

    @classmethod
    def get_api_config(cls) -> dict:
        """Get API configuration dictionary."""
        return {
            "base_url": cls.CROPWISE_API_BASE_URL,
            "api_key": cls.CROPWISE_API_KEY,
            "default_source": cls.DEFAULT_SOURCE,
            "default_country": cls.DEFAULT_COUNTRY,
            "default_org_id": cls.DEFAULT_ORG_ID,
        }

    @classmethod
    def get_operation_defaults(cls) -> dict:
        """Get default operation parameters."""
        return {
            "batch_size": cls.DEFAULT_BATCH_SIZE,
            "cache_duration": cls.DEFAULT_CACHE_DURATION,
            "source": cls.DEFAULT_SOURCE,
            "country": cls.DEFAULT_COUNTRY,
        }

    @classmethod
    def validate_required_config(cls) -> bool:
        """Validate that required configuration is present."""
        missing_configs = []

        if not cls.CROPWISE_API_BASE_URL:
            missing_configs.append("CROPWISE_API_BASE_URL")

        if not cls.CROPWISE_API_KEY:
            missing_configs.append("CROPWISE_API_KEY")

        if missing_configs:
            raise ValueError(
                f"Missing required configuration: {', '.join(missing_configs)}"
            )

        return True
