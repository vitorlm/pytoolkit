from pydantic import BaseModel, Field


class ValYouRecognition(BaseModel):
    """Represents a single Val-You recognition received by a team member.

    Parsed from the Val-You CSV export file containing recognition data.

    Attributes:
        sender_first_name: First name of the recognition sender.
        sender_last_name: Last name of the recognition sender.
        sender_department: Department of the sender.
        sender_country: Country of the sender.
        status: Recognition status (e.g., "Aprovada").
        award_type: Award tier (Thank You / Opal / Topaz / Sapphire).
        award_reason: Company value category for the award.
        title: Recognition title.
        message: Recognition message text.
        privacy: Privacy setting of the recognition.
        points: Points associated with the recognition.
    """

    sender_first_name: str
    sender_last_name: str
    sender_department: str = ""
    sender_country: str = ""
    status: str = ""
    award_type: str = ""
    award_reason: str = ""
    title: str = ""
    message: str = ""
    privacy: str = ""
    points: int = 0


class MemberRecognitions(BaseModel):
    """Aggregated Val-You recognition data for a single team member.

    Attributes:
        total_count: Total number of recognitions received.
        recognitions: List of individual ValYouRecognition records.
        senders: Unique list of sender full names.
        award_type_breakdown: Count of recognitions per award tier.
        award_reason_breakdown: Count of recognitions per company value.
    """

    total_count: int = 0
    recognitions: list[ValYouRecognition] = Field(default_factory=list)
    senders: list[str] = Field(default_factory=list)
    award_type_breakdown: dict[str, int] = Field(default_factory=dict)
    award_reason_breakdown: dict[str, int] = Field(default_factory=dict)
