import logging
from states import SINGLE_MEMBER_STATES
from utils import chunk, FEC_fetch, pick


def get_missing_recipient_data(recipients, db, session):
    committee_data = {}

    needs_data_ids = [k for k, v in recipients.items() if v.get("needs_data", False)]
    for ids_chunk in chunk(needs_data_ids, 10):
        data = FEC_fetch(
            session,
            "committee",
            "https://api.open.fec.gov/v1/committees/",
            params={"committee_id": ids_chunk},
        )
        for committee in data["results"]:
            committee_id = committee["committee_id"]
            committee_data[committee_id] = {
                "committee_name": committee["name"],
                "party": committee["party"],
                "state": committee["state"],
                "designation_full": committee["designation_full"],
                "committee_type_full": committee["committee_type_full"],
                "candidate_ids": committee["candidate_ids"],
                "sponsor_candidate_ids": committee["sponsor_candidate_ids"],
            }

    candidate_data = {}
    for recipient_id in recipients.keys():
        if recipient_id in db.committees:
            recipients[recipient_id]["link"] = "/committees/" + recipient_id
        if recipient_id in db.all_committees:
            recipients[recipient_id]["description"] = db.all_committees[recipient_id]
        if recipient_id in committee_data:
            recipients[recipient_id] = {
                **recipients[recipient_id],
                **committee_data[recipient_id],
            }
            del recipients[recipient_id]["needs_data"]
        if recipient_id in db.committee_affiliations:
            try:
                recipients[recipient_id] = {
                    **recipients.get(recipient_id, {}),
                    **db.committee_affiliations.get(recipient_id, {}),
                }
            except Exception as e:
                logging.error(
                    "Malformed committee affiliation", {"recipient_id": recipient_id}
                )
                print(f"Malformed committee affiliation: {recipient_id}")
        if recipients[recipient_id].get("candidate_ids") is not None:
            for candidate_id in recipients[recipient_id]["candidate_ids"]:
                if (
                    candidate_id
                    not in recipients.get(recipient_id, {}).get("candidate_details", {})
                    and candidate_id[0] in {"P", "H", "S"}
                ) and candidate_id not in candidate_data:
                    candidate_data[candidate_id] = {}
        if recipients[recipient_id].get("sponsor_candidate_ids") is not None:
            for candidate_id in recipients[recipient_id]["sponsor_candidate_ids"]:
                if (
                    candidate_id not in recipients[recipient_id]["candidate_details"]
                    and candidate_id not in candidate_data
                    and candidate_id[0] in {"P", "H", "S"}
                ):
                    candidate_data[candidate_id] = {}

    for ids_chunk in chunk(list(candidate_data.keys()), 10):
        data = FEC_fetch(
            session,
            "candidate",
            "https://api.open.fec.gov/v1/candidates/",
            params={"candidate_id": ids_chunk},
        )
        for candidate in data["results"]:
            candidate_id = candidate["candidate_id"]
            candidate_data[candidate_id] = pick(
                candidate,
                [
                    "name",
                    "party",
                    "state",
                    "office",
                    "district",
                    "incumbent_challenge",
                    "election_years",
                ],
            )
            candidate_data[candidate_id]["isRunningThisCycle"] = (
                2026 in candidate["election_years"]
            )
            race_doc = (
                db.client.collection("raceDetails").document(candidate["state"]).get()
            )
            race_data = race_doc.to_dict() if race_doc.exists else None
            if race_data:
                if candidate["office"] == "S" and "S" in race_data:
                    candidate_data[candidate_id][
                        "race_link"
                    ] = f"/elections/{candidate['state']}-S"
                elif candidate["office"] == "H":
                    district = (
                        candidate["district"]
                        if candidate["state"] not in SINGLE_MEMBER_STATES
                        else "01"
                    )
                    if f"H-{district}" in race_data:
                        candidate_data[candidate_id][
                            "race_link"
                        ] = f"/elections/{candidate['state']}-H-{district}"
                elif candidate["office"] == "P":
                    candidate_data[candidate_id]["race_link"] = "/elections/president"

    for recipient_id in recipients.keys():
        related_candidates = recipients[recipient_id].get("candidate_ids", []) or []
        if recipients[recipient_id].get("sponsor_candidate_ids") is not None:
            related_candidates.extend(recipients[recipient_id]["sponsor_candidate_ids"])
        if "candidate_details" not in recipients[recipient_id]:
            recipients[recipient_id]["candidate_details"] = {}
        for candidate_id in related_candidates:
            if candidate_id in candidate_data:
                recipients[recipient_id]["candidate_details"][
                    candidate_id
                ] = candidate_data[candidate_id]
    return recipients
