def _merge_candidate(existing, new_entry, race_id):
    """Merge a second-race candidate entry into an existing one.

    Sums expenditure totals, outside-spending totals, and spending lists.
    Prefers the regular (non-special) race as the primary race field so that
    the candidate's page links to their upcoming election.
    """
    existing["support_total"] = round(
        existing["support_total"] + new_entry["support_total"], 2
    )
    existing["oppose_total"] = round(
        existing["oppose_total"] + new_entry["oppose_total"], 2
    )

    # Merge Schedule-E outside spending if present.
    new_os = new_entry.get("outside_spending")
    if new_os:
        ex_os = existing.get("outside_spending")
        if ex_os:
            ex_os["support_total"] = round(
                ex_os.get("support_total", 0) + new_os.get("support_total", 0), 2
            )
            ex_os["oppose_total"] = round(
                ex_os.get("oppose_total", 0) + new_os.get("oppose_total", 0), 2
            )
            ex_os["support"] = ex_os.get("support", []) + new_os.get("support", [])
            ex_os["oppose"] = ex_os.get("oppose", []) + new_os.get("oppose", [])
        else:
            existing["outside_spending"] = new_os

    # Prefer the regular (non-special) race so the candidate's page links to
    # the upcoming election rather than the completed special election.
    if existing["race"].endswith("-special") and not race_id.endswith("-special"):
        existing["race"] = race_id


def update_candidates_expenditures(db):
    candidates = {}
    race_details = db.client.collection("raceDetails").stream()
    docs = [state for state in race_details]
    for state in docs:
        state, state_data = state.id, state.to_dict()
        if state == "US":
            continue
        for race_id, race in state_data.items():
            for candidate_name, candidate in race["candidates"].items():
                if candidate["support_total"] > 0 or candidate["oppose_total"] > 0:
                    if candidate_name not in candidates:
                        candidates[candidate_name] = {
                            **candidate,
                            "state": state,
                            "race": race_id,
                        }
                    else:
                        _merge_candidate(candidates[candidate_name], candidate, race_id)

    candidates_list = sorted(
        [
            [name, c["support_total"] + c["oppose_total"]]
            for name, c in candidates.items()
        ],
        key=lambda x: x[1],
        reverse=True,
    )
    ordered_candidates = [c[0] for c in candidates_list]
    for name, candidate in candidates.items():
        db.client.collection("candidates").document(name).set(candidate)
    db.client.collection("candidatesOrder").document("order").set(
        {"order": ordered_candidates}, merge=True
    )
