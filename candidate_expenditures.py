def update_candidates_expenditures(db):
    candidates = {}
    candidates_list = []
    race_details = db.client.collection("raceDetails").stream()
    for state in race_details:
        state, state_data = state.id, state.to_dict()
        for race_id, race in state_data.items():
            for candidate_name, candidate in race["candidates"].items():
                if candidate["support_total"] > 0 or candidate["oppose_total"] > 0:
                    candidates[candidate_name] = {
                        **candidate,
                        "state": state,
                        "race": race_id,
                    }
                    candidates_list.append(
                        [
                            candidate_name,
                            candidate["support_total"] + candidate["oppose_total"],
                        ]
                    )

    candidates_list.sort(key=lambda x: x[1], reverse=True)
    ordered_candidates = [c[0] for c in candidates_list]
    db.client.collection("candidates").document("bySpending").set(
        {"candidates": candidates, "order": ordered_candidates}
    )
