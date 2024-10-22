import statistics


def is_below_median(candidate_summary, median_raised):
    return (
        median_raised
        and candidate_summary
        and "raised_total" in candidate_summary
        and candidate_summary["raised_total"] < median_raised
    )


def find_index_to_slice(candidate_list, candidate_data):
    total_raised = [
        candidate_data[candidate["name"]]["raised_total"]
        for candidate in candidate_list
        if (
            candidate["name"] in candidate_data
            and "raised_total" in candidate_data[candidate["name"]]
        )
    ]
    median_raised = None
    if len(total_raised):
        median_raised = min(statistics.median(total_raised), 1000000)

    slice_ind = None
    supported_indices = []
    for ind, candidate in enumerate(candidate_list):
        name = candidate["name"]
        candidate_summary = candidate_data[name] if name in candidate_data else None
        if ind > 2 and slice_ind == None:
            if "percentage" in candidate:
                if candidate["percentage"] < 5 and is_below_median(
                    candidate_summary, median_raised
                ):
                    # Remove candidates with <5% votes if they raised below the median amount
                    slice_ind = ind
            elif is_below_median(candidate_summary, median_raised):
                # Remove candidates who raised below the median amount IF the vote hasn't happened yet
                slice_ind = ind
        if candidate_summary and (
            candidate_summary.get("support_total", 0) > 0
            or candidate_summary.get("oppose_total", 0) > 0
            or candidate_summary.get("has_non_pac_support", False)
        ):
            # However, don't remove candidates who have received support or opposition
            supported_indices.append(ind + 1)

    if slice_ind:
        if len(supported_indices):
            to_slice = max(slice_ind, max(supported_indices))
        else:
            to_slice = slice_ind

        if to_slice < len(candidate_list) - 1:
            return to_slice
    return None


def trim_candidates(db):
    race_docs = db.client.collection("raceDetails").stream()
    for doc in race_docs:
        modified_state = False
        state, state_data = doc.id, doc.to_dict()

        for race_id, race_data in state_data.items():
            modified_race = False
            for ind, race in enumerate(race_data["races"]):
                sorted_race_candidates = race["candidates"]
                if not any("won" in candidate for candidate in race["candidates"]):
                    # This is an upcoming race, so it's not sorted by outcome.
                    # Sort instead by amount raised.
                    sorted_race_candidates = sorted(
                        race["candidates"],
                        key=lambda c: race_data["candidates"]
                        .get(c["name"], {})
                        .get("raised_total", 0),
                        reverse=True,
                    )
                    state_data[race_id]["races"][ind][
                        "candidates"
                    ] = sorted_race_candidates
                    modified_race = True
                    modified_state = True

                slice_index = find_index_to_slice(
                    sorted_race_candidates, race_data["candidates"]
                )
                if slice_index:
                    state_data[race_id]["races"][ind][
                        "candidates"
                    ] = sorted_race_candidates[:slice_index]
                    modified_race = True
                    modified_state = True
            if modified_race:
                remaining_candidates = {
                    candidate["name"]
                    for race in state_data[race_id]["races"]
                    for candidate in race["candidates"]
                }
                candidates = list(state_data[race_id]["candidates"].keys())
                for candidate in candidates:
                    if candidate not in remaining_candidates:
                        del state_data[race_id]["candidates"][candidate]

        if modified_state:
            db.client.collection("raceDetails").document(state).set(state_data)
