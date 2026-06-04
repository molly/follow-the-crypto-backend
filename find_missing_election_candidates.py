"""
Find candidates who appear in the "Other supported races" list
(candidatesWithoutExpendituresOrder, isRunningThisCycle=True)
but have no matching candidate_id in raceDetails.
These are the candidates with the broken display (no photo, raw FEC ID shown).
"""

from Database import Database
SINGLE_MEMBER_STATES = {"AK", "DE", "ND", "SD", "VT", "VI", "WY"}


def main():
    db = Database()

    # Collect all candidate_ids present in raceDetails
    print("Loading raceDetails...")
    race_candidate_ids = set()
    for doc in db.client.collection("raceDetails").stream():
        data = doc.to_dict()
        for race_data in data.values():
            if not isinstance(race_data, dict):
                continue
            for name, candidate in race_data.get("candidates", {}).items():
                if not isinstance(candidate, dict):
                    continue
                cid = candidate.get("candidate_id")
                if cid:
                    race_candidate_ids.add(cid)

    print(f"Found {len(race_candidate_ids)} candidate IDs in raceDetails")

    # Get the candidatesWithoutExpendituresOrder list (what feeds the "Other races" section)
    print("Loading candidatesWithoutExpendituresOrder...")
    order_doc = db.client.collection("allRecipients").document("recipientsOrder").get().to_dict()
    candidates_without_exp = set(order_doc.get("candidatesWithoutExpendituresOrder", []))
    print(f"Found {len(candidates_without_exp)} candidates in candidatesWithoutExpendituresOrder")

    # Load recipientDetails for candidates in that list
    print("Loading recipientDetails...")
    missing = []
    for doc in db.client.collection("recipientDetails").stream():
        candidate_id = doc.id
        if candidate_id not in candidates_without_exp:
            continue
        if not (candidate_id.startswith("H") or candidate_id.startswith("S")):
            continue
        recipient = doc.to_dict()
        details = recipient.get("candidate_details", {})
        if not isinstance(details, dict):
            continue
        if not details.get("isRunningThisCycle"):
            continue
        office = details.get("office")
        if office not in ("H", "S"):
            continue
        if candidate_id not in race_candidate_ids:
            name = details.get("name", "")
            state = details.get("state", "")
            district = details.get("district", "")
            total = recipient.get("total", 0)
            race = f"{state}-{office}"
            if office == "H" and district and state not in SINGLE_MEMBER_STATES:
                race += f"-{district}"
            missing.append({
                "candidate_id": candidate_id,
                "name": name,
                "state": state,
                "office": office,
                "district": district,
                "race": race,
                "total": total,
            })

    missing.sort(key=lambda x: -x["total"])

    print(f"\nFound {len(missing)} candidates in 'Other races' with no raceDetails entry:\n")
    header = f"{'Candidate ID':<14}  {'Name':<35}  {'Race':<12}  {'Total':>10}"
    separator = "-" * 80
    rows = [f"{m['candidate_id']:<14}  {m['name']:<35}  {m['race']:<12}  ${m['total']:>9,.0f}" for m in missing]

    print(header)
    print(separator)
    for row in rows:
        print(row)

    output_path = "missing_election_candidates.txt"
    with open(output_path, "w") as f:
        f.write(f"Found {len(missing)} candidates in 'Other races' with no raceDetails entry:\n\n")
        f.write(header + "\n")
        f.write(separator + "\n")
        f.write("\n".join(rows) + "\n")
    print(f"\nSaved to {output_path}")


if __name__ == "__main__":
    main()
