import json
from datetime import UTC, datetime

# Check one of the output files
with open("output/2025_assessment/members/Elias/stats.json") as f:
    data = json.load(f)

kudos = data.get("kudos")
if kudos and kudos["total_count"] > 0:
    print("Member: Elias")
    print(f"Period: {kudos.get('period', 'N/A')}")
    print(f"Total kudos: {kudos['total_count']}")
    print("\nKudos timestamps and years:")
    for k in kudos["kudos"]:
        ts = float(k["timestamp"])
        dt = datetime.fromtimestamp(ts, tz=UTC)
        print(f"  - {k['sender']}: {dt.strftime('%Y-%m-%d')} (year={dt.year})")

# Also check Andrea
print("\n" + "=" * 50)
with open("output/2025_assessment/members/Andrea/stats.json") as f:
    data = json.load(f)

kudos = data.get("kudos")
if kudos and kudos["total_count"] > 0:
    print("Member: Andrea")
    print(f"Period: {kudos.get('period', 'N/A')}")
    print(f"Total kudos: {kudos['total_count']}")
    print("\nKudos timestamps and years:")
    for k in kudos["kudos"]:
        ts = float(k["timestamp"])
        dt = datetime.fromtimestamp(ts, tz=UTC)
        print(f"  - {k['sender']}: {dt.strftime('%Y-%m-%d')} (year={dt.year})")
