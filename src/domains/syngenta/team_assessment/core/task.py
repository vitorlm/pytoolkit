from datetime import date
from typing import Optional
from pydantic import BaseModel


class Task(BaseModel):
    """
    Model for validating task data.

    Attributes:
        code (str): Task code.
        jira (Optional[str]): JIRA issue key.
        description (Optional[str]): Task description.
        type (Literal["Eng", "Prod", "Day-by-Day", "Out", "Bug"]): Task type.
        start_date (Optional[date]): Task start date.
        end_date (Optional[date]): Task end date.
        execution_duration (Optional[int]): Task execution duration in days.
    """

    code: str
    jira: Optional[str] = None
    description: Optional[str] = None
    type: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    execution_duration: Optional[int] = None

    def __hash__(self):
        """
        Generates a hash value based on the task's code, jira, and description.

        Returns:
            int: Hash value.
        """
        return hash((self.code, self.jira, self.description, self.type))

    def __eq__(self, other):
        """
        Compares two Task objects for equality.

        Args:
            other (Task): Another Task object.

        Returns:
            bool: True if both objects are equal, False otherwise.
        """
        if not isinstance(other, Task):
            return False
        return (
            self.code == other.code
            and self.jira == other.jira
            and self.description == other.description
            and self.type == other.type
        )
