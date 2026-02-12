from pydantic import BaseModel, Field


class WorkdayRatingPair(BaseModel):
    """Holds WHAT and HOW ratings from Workday performance evaluations.

    Valid values for ratings are typically "PA", "PE", "EX", or None.
    """

    what: str | None = None
    what_comment: str | None = None
    how: str | None = None
    how_comment: str | None = None


class WorkdayRatings(BaseModel):
    """Holds manager and optional self-evaluation ratings."""

    manager: WorkdayRatingPair = Field(default_factory=WorkdayRatingPair)
    self_eval: WorkdayRatingPair | None = None


class WorkdayFeedback(BaseModel):
    """Represents a written feedback extracted from Workday PDFs."""

    authors: list[str] = Field(default_factory=list)
    text: str = ""


class WorkdayData(BaseModel):
    """Top-level model for all Workday-related data for a member."""

    ratings: WorkdayRatings | None = None
    official_feedback: list[WorkdayFeedback] = Field(default_factory=list)
