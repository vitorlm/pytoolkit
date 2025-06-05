import os

from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))


class JiraConfig:
    def __init__(self):
        self._base_url = os.getenv("JIRA_URL")
        self._email = os.getenv("JIRA_USER_EMAIL")
        self._api_token = os.getenv("JIRA_API_TOKEN")

    @property
    def base_url(self):
        return self._base_url

    @property
    def email(self):
        return self._email

    @property
    def api_token(self):
        return self._api_token
