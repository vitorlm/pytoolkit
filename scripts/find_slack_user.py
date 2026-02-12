"""Find Slack User IDs for team members."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / "src" / "domains" / "syngenta" / "team_assessment" / ".env")

import os

from slack_sdk import WebClient

SEARCH_NAMES = [
    "Italo",
    "Rafal",
    "Rafael Milanez",
    "Andrea Zambrana",
    "Andrea",
    "Zambrana",
    "Ortega",
    "Milanez",
]

client = WebClient(token=os.getenv("SLACK_BOT_TOKEN"))

# Build search terms (lowercase first/last name fragments)
search_terms: list[tuple[str, list[str]]] = []
for full_name in SEARCH_NAMES:
    parts = [p.lower() for p in full_name.split()]
    search_terms.append((full_name, parts))

found: dict[str, list[dict]] = {n: [] for n in SEARCH_NAMES}

cursor = None
page = 0
while True:
    page += 1
    kwargs: dict = {"limit": 200}
    if cursor:
        kwargs["cursor"] = cursor
    resp = client.users_list(**kwargs)
    members = resp["members"]
    print(f"Page {page}: {len(members)} users")

    for m in members:
        real_name = m.get("real_name", "") or ""
        display_name = m.get("profile", {}).get("display_name", "") or ""
        name_lower = real_name.lower()
        display_lower = display_name.lower()

        for full_name, parts in search_terms:
            if all(p in name_lower or p in display_lower for p in parts):
                found[full_name].append(
                    {
                        "id": m["id"],
                        "real_name": real_name,
                        "display_name": display_name,
                        "email": m.get("profile", {}).get("email", "N/A"),
                    }
                )

    cursor = resp.get("response_metadata", {}).get("next_cursor", "")
    if not cursor:
        break

print("\n--- Results ---")
for name in SEARCH_NAMES:
    matches = found[name]
    if not matches:
        print(f"\n{name}: NOT FOUND")
    else:
        for hit in matches:
            print(f"\n{name}:")
            print(f"  ID:      {hit['id']}")
            print(f"  Name:    {hit['real_name']}")
            print(f"  Display: {hit['display_name']}")
            print(f"  Email:   {hit['email']}")
