"""Base Pydantic models for MCP tool validation."""

from pydantic import BaseModel, ConfigDict, Field


class BaseMCPModel(BaseModel):
    """Base Pydantic model for all MCP tool arguments.

    Provides common configuration and validation patterns
    for all MCP tool parameter validation.
    """

    model_config = ConfigDict(
        # Don't allow extra fields to prevent parameter typos
        extra="forbid",
        # Strip whitespace from strings
        str_strip_whitespace=True,
        # Use enum values instead of enum objects
        use_enum_values=True,
        # Validate default values
        validate_default=True,
        # Validate assignment operations
        validate_assignment=True,
    )


class OutputFileModel(BaseModel):
    """Common output file parameters."""

    output_file: str | None = Field(
        None,
        description="Optional file path to save results in JSON format",
        min_length=1,
    )

    verbose: bool | None = Field(
        False,
        description="Enable verbose output with detailed information",
    )


class CacheControlModel(BaseModel):
    """Common cache control parameters."""

    clear_cache: bool | None = Field(
        False,
        description="Clear cache before executing operation to get fresh data",
    )
