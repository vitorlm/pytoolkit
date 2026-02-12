import json
import os

members_dir = "output/2025_assessment/members"
for m in sorted(os.listdir(members_dir)):
    f = os.path.join(members_dir, m, "stats.json")
    if os.path.exists(f):
        with open(f) as fh:
            d = json.load(fh)
        kudos = d.get("kudos")
        valyou = d.get("valyou_recognitions")
        k_count = kudos["total_count"] if kudos else 0
        v_count = valyou["total_count"] if valyou else 0
        v_senders = valyou["senders"] if valyou else []
        v_types = valyou["award_type_breakdown"] if valyou else {}
        k_senders = kudos["senders"] if kudos else []
        print(f"{m}:")
        print(f"  Kudos: {k_count}, senders={k_senders}")
        print(f"  Val-You: {v_count}, senders={v_senders}")
        print(f"  Val-You types: {v_types}")
        print()
