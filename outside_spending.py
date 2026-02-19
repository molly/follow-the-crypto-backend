import logging
from utils import FEC_fetch, pick, get_expenditure_race_type

SCHEDULE_E_FIELDS = [
    "expenditure_amount",
    "expenditure_date",
    "expenditure_description",
    "support_oppose_indicator",
    "candidate_id",
    "category_code",
    "category_code_full",
    "committee_id",
    "pdf_url",
    "transaction_id",
    "subrace",
]


def split_into_chunks(array):
    """Split into chunks of max length 10, to handle limit on number of candidate IDs"""
    return [array[i : i + 10] for i in range(0, len(array), 10)]


def update_candidate_outside_spending(db, session):
    try:
        race_docs = db.client.collection("raceDetails").stream()
        docs = [doc for doc in race_docs]
        for doc in docs:
            state, state_data = doc.id, doc.to_dict()
            if state == "US":
                continue
            for race_id, race_data in state_data.items():
                candidate_ids = [
                    candidate["candidate_id"]
                    for candidate in race_data["candidates"].values()
                    if "candidate_id" in candidate
                ]
                candidate_id_chunks = split_into_chunks(candidate_ids)
                outside_spending = {}

                # When both a regular race and a special race exist for the
                # same seat (e.g. H-06 and H-06-special), the same candidate
                # can appear in both.  Filter by election_type so that
                # S-typed expenditures (special election) only count toward
                # the special race and all others count toward the regular race.
                is_special_race = race_id.endswith("-special")
                base_race_id = race_id[: -len("-special")] if is_special_race else race_id
                has_both_races = (
                    base_race_id in state_data
                    and f"{base_race_id}-special" in state_data
                )

                for chunk in candidate_id_chunks:
                    last_index = None
                    last_expenditure_date = None
                    result_count = 0
                    transaction_ids = set()
                    while True:
                        candidate_data = FEC_fetch(
                            session,
                            "outside spending for candidates",
                            "https://api.open.fec.gov/v1/schedules/schedule_e",
                            {
                                "candidate_id": chunk,
                                "per_page": 100,
                                "cycle": 2026,
                                "is_notice": True,
                                "most_recent": True,
                                "last_index": last_index,
                                "last_expenditure_date": last_expenditure_date,
                            },
                        )
                        result_count += candidate_data["pagination"]["per_page"]

                        for result in candidate_data["results"]:
                            if result["memoed_subtotal"]:
                                # Avoid double-counting memoed items
                                continue
                            # When a race has both a regular and special
                            # counterpart, route each expenditure to the
                            # correct one based on election_type.
                            if has_both_races:
                                exp_election_type = result.get("election_type") or ""
                                exp_is_special = exp_election_type.startswith("S")
                                if is_special_race and not exp_is_special:
                                    continue
                                if not is_special_race and exp_is_special:
                                    continue
                            result["subrace"] = get_expenditure_race_type(
                                result, race_data["races"]
                            )
                            candidate_id = result["candidate_id"]
                            match = next(
                                candidate["common_name"]
                                for candidate in race_data["candidates"].values()
                                if candidate.get("candidate_id") == candidate_id
                            )
                            if match:
                                if match not in outside_spending:
                                    outside_spending[match] = {
                                        "support": [],
                                        "oppose": [],
                                        "support_total": 0,
                                        "oppose_total": 0,
                                    }
                                if result["support_oppose_indicator"] == "S":
                                    outside_spending[match]["support"].append(
                                        pick(result, SCHEDULE_E_FIELDS)
                                    )
                                    outside_spending[match]["support_total"] += result[
                                        "expenditure_amount"
                                    ]
                                    transaction_ids.add(result["transaction_id"])
                                elif result["support_oppose_indicator"] == "O":
                                    outside_spending[match]["oppose"].append(
                                        pick(result, SCHEDULE_E_FIELDS)
                                    )
                                    outside_spending[match]["oppose_total"] += result[
                                        "expenditure_amount"
                                    ]
                                    transaction_ids.add(result["transaction_id"])

                            else:
                                logging.error(
                                    f"Couldn't find candidate for outside expenditure: {candidate_id}"
                                )
                                print(
                                    f"Couldn't find candidate for outside expenditure: {candidate_id}"
                                )

                        # Fetch another page if needed
                        if result_count <= candidate_data["pagination"]["count"]:
                            last_expenditure_date = candidate_data["pagination"][
                                "last_indexes"
                            ]["last_expenditure_date"]
                            last_index = candidate_data["pagination"]["last_indexes"][
                                "last_index"
                            ]
                        else:
                            break

                    page = 1
                    while True:
                        candidate_data = FEC_fetch(
                            session,
                            "outside spending for candidates",
                            "https://api.open.fec.gov/v1/schedules/schedule_e/efile",
                            {
                                "candidate_id": chunk,
                                "per_page": 100,
                                "min_date": "2025-01-01",
                                "is_notice": True,
                                "most_recent": True,
                                "page": page,
                            },
                        )
                        for result in candidate_data["results"]:
                            amendment = False
                            if result["transaction_id"] in transaction_ids:
                                if result["amendment_indicator"] == "A":
                                    # This was amended, so replace the transaction from above.
                                    amendment = True
                                else:
                                    continue
                            # Same special/regular filtering as the main loop above.
                            if has_both_races:
                                exp_election_type = result.get("election_type") or ""
                                exp_is_special = exp_election_type.startswith("S")
                                if is_special_race and not exp_is_special:
                                    continue
                                if not is_special_race and exp_is_special:
                                    continue
                            result["subrace"] = get_expenditure_race_type(
                                result, race_data["races"]
                            )
                            candidate_id = result["candidate_id"]
                            match = next(
                                candidate["common_name"]
                                for candidate in race_data["candidates"].values()
                                if candidate.get("candidate_id") == candidate_id
                            )
                            if match:
                                if match not in outside_spending:
                                    outside_spending[match] = {
                                        "support": [],
                                        "oppose": [],
                                        "support_total": 0,
                                        "oppose_total": 0,
                                    }
                                if result["support_oppose_indicator"] == "S":
                                    if amendment:
                                        try:
                                            old_index = next(
                                                i
                                                for i, item in enumerate(
                                                    outside_spending[match]["support"]
                                                )
                                                if item["transaction_id"]
                                                == result["transaction_id"]
                                            )
                                            outside_spending[match][
                                                "support_total"
                                            ] -= outside_spending[match]["support"][
                                                old_index
                                            ][
                                                "expenditure_amount"
                                            ]
                                            outside_spending[match]["support"][
                                                old_index
                                            ] = pick(result, SCHEDULE_E_FIELDS)
                                            outside_spending[match][
                                                "support_total"
                                            ] += result["expenditure_amount"]
                                        except StopIteration:
                                            outside_spending[match]["support"].append(
                                                pick(result, SCHEDULE_E_FIELDS)
                                            )
                                            outside_spending[match][
                                                "support_total"
                                            ] += result["expenditure_amount"]
                                    else:
                                        outside_spending[match]["support"].append(
                                            pick(result, SCHEDULE_E_FIELDS)
                                        )
                                        outside_spending[match][
                                            "support_total"
                                        ] += result["expenditure_amount"]
                                elif result["support_oppose_indicator"] == "O":
                                    if amendment:
                                        try:
                                            old_index = next(
                                                i
                                                for i, item in enumerate(
                                                    outside_spending[match]["oppose"]
                                                )
                                                if item["transaction_id"]
                                                == result["transaction_id"]
                                            )
                                            outside_spending[match][
                                                "oppose_total"
                                            ] -= outside_spending[match]["oppose"][
                                                old_index
                                            ][
                                                "expenditure_amount"
                                            ]
                                            outside_spending[match]["oppose"][
                                                old_index
                                            ] = pick(result, SCHEDULE_E_FIELDS)
                                            outside_spending[match][
                                                "oppose_total"
                                            ] += result["expenditure_amount"]
                                        except:
                                            outside_spending[match]["oppose"].append(
                                                pick(result, SCHEDULE_E_FIELDS)
                                            )
                                            outside_spending[match][
                                                "oppose_total"
                                            ] += result["expenditure_amount"]
                                    else:
                                        outside_spending[match]["oppose"].append(
                                            pick(result, SCHEDULE_E_FIELDS)
                                        )
                                        outside_spending[match][
                                            "oppose_total"
                                        ] += result["expenditure_amount"]

                            else:
                                logging.error(
                                    f"Couldn't find candidate for outside expenditure: {candidate_id}"
                                )
                                print(
                                    f"Couldn't find candidate for outside expenditure: {candidate_id}"
                                )

                        # Fetch another page if needed
                        if page < candidate_data["pagination"]["pages"]:
                            page += 1
                        else:
                            break

                for candidate_name, candidate_spending in outside_spending.items():
                    state_data[race_id]["candidates"][candidate_name][
                        "outside_spending"
                    ] = candidate_spending
            db.client.collection("raceDetails").document(state).set(state_data)
    except Exception as e:
        logging.error(f"Error updating outside spending: {e}")
        print(f"Error updating outside spending: {e}")
        raise e
