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
    """

    code: str
    jira: Optional[str]
    description: Optional[str]
    type: str

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
