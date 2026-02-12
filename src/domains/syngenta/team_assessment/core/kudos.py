from pydantic import BaseModel, Field


class Kudos(BaseModel):
    """Represents a single kudos received by a team member.

    Parsed from Appreci bot messages in the #global-kudos Slack channel.

    Attributes:
        sender: Name of the person who sent the kudos.
        message: The kudos message text (free-text content).
        timestamp: Slack message timestamp (ts).
        permalink: Permalink URL to the Slack message.
    """

    sender: str
    message: str = ""
    timestamp: str
    permalink: str = ""


class MemberKudos(BaseModel):
    """Aggregated kudos data for a single team member.

    Attributes:
        total_count: Total number of kudos received.
        kudos: List of individual Kudos records.
        senders: Unique list of sender names.
        period: The assessment period (e.g., '2025').
    """

    total_count: int = 0
    kudos: list[Kudos] = Field(default_factory=list)
    senders: list[str] = Field(default_factory=list)
    period: str = ""
