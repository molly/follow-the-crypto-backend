import logging
from utils import FEC_fetch


def update_candidate_outside_spending(db):
    race_docs = db.client.collection("raceDetails").stream()
    for doc in race_docs:
        state, state_data = doc.id, doc.to_dict()
        for race_id, race_data in state_data.items():
            candidate_ids = [
                candidate["candidate_id"]
                for candidate in race_data["candidates"].values()
                if "candidate_id" in candidate
            ]
            candidate_data = FEC_fetch(
                "outside spending for candidates",
                "https://api.open.fec.gov/v1/schedules/schedule_e/by_candidate",
                {
                    "candidate_id": candidate_ids,
                    "per_page": 100,
                    "cycle": 2024,
                    "is_notice": True,
                },
            )
            outside_spending = {}
            if candidate_data["pagination"]["pages"] > 1:
                # TODO paginate
                print("has more")
            for result in candidate_data["results"]:
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
                        outside_spending[match]["support"].append(result)
                        outside_spending[match]["support_total"] += result["total"]
                    elif result["support_oppose_indicator"] == "O":
                        outside_spending[match]["oppose"].append(result)
                        outside_spending[match]["oppose_total"] += result["total"]

                else:
                    logging.error(
                        f"Couldn't find candidate for outside expenditure: {candidate_id}"
                    )
                    print(
                        f"Couldn't find candidate for outside expenditure: {candidate_id}"
                    )
            for candidate_name, candidate_spending in outside_spending.items():
                state_data[race_id]["candidates"][candidate_name][
                    "outside_spending"
                ] = candidate_spending
        db.client.collection("raceDetails").document(state).set(state_data)
