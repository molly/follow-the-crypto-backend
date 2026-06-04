"""
Surface races that have recorded spending but no matching raceDetails entry.

These are races where the frontend shows a spending total (e.g. "$41k from PACs")
but no candidate/election detail, because the race-detail scrape
(tasks/races.py::update_race_details) hasn't run since the spending landed.

Two buckets are reported:
  - PAC spending with no raceDetails: races with super-PAC independent
    expenditures (expenditures.states[state].by_race) and no detail. These are
    the ones that cause the "spending but no detail" bug and should be hydrated.
  - Company-only contributions with no raceDetails: races that only have direct
    company contributions (by_race_companies). These are not scraped by design,
    so they're listed separately for reference, not as bugs.

To fix the PAC bucket, re-run the race-detail pipeline:
    python pipeline.py --tasks compute_race_insights
(which pulls update_race_details -> summarize_races -> prune_race_details).

Run directly:
    python -m scripts.find_unhydrated_races
"""

import re


def _short_id(state, race_id):
    """Convert a full race id ("UT-H-03") to its raceDetails short id ("H-03")."""
    parts = race_id.split("-")
    return "-".join(parts[1:]) if parts[0] == state else race_id


def find_unhydrated_races(db):
    # Build the set of existing raceDetails short ids per state. raceDetails docs
    # are sharded as "<STATE>_<n>" (e.g. UT_0); merge them back per state.
    existing = {}  # state -> set(short_id)
    for doc in db.client.collection("raceDetails").stream():
        match = re.match(r"^(.*)_(\d)$", doc.id)
        state = match.group(1) if match else doc.id
        existing.setdefault(state, set()).update(doc.to_dict().keys())

    state_data = (
        db.client.collection("expenditures").document("states").get().to_dict()
    )

    missing_pac = []
    missing_company = []
    for state, data in state_data.items():
        if state in ("US", "None", None):
            continue
        have = existing.get(state, set())
        by_race = data.get("by_race", {})
        for race_id, race in by_race.items():
            if _short_id(state, race_id) not in have:
                missing_pac.append((race_id, race.get("total", 0)))
        for race_id, total in data.get("by_race_companies", {}).items():
            # Skip races already flagged via the PAC bucket above.
            if _short_id(state, race_id) in have or race_id in by_race:
                continue
            amount = total if isinstance(total, (int, float)) else sum(total.values())
            missing_company.append((race_id, amount))

    print(f"=== Races with PAC spending but NO raceDetails ({len(missing_pac)}) ===")
    for race_id, total in sorted(missing_pac, key=lambda x: -x[1]):
        print(f"  {race_id:18} ${total:,.0f}")

    print(
        f"\n=== Races with ONLY company contributions and no raceDetails "
        f"({len(missing_company)}) ==="
    )
    for race_id, total in sorted(missing_company, key=lambda x: -x[1]):
        print(f"  {race_id:18} ${total:,.0f}")

    return {"missing_pac": missing_pac, "missing_company": missing_company}


if __name__ == "__main__":
    from Database import Database

    find_unhydrated_races(Database())
