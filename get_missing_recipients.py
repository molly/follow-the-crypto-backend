import logging
from states import SINGLE_MEMBER_STATES
from utils import chunk, FEC_fetch, pick


def race_has_candidate(race, candidate_id):
    return any(
        candidate_id == c.get("candidate_id", None) for c in race["candidates"].values()
    )


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
            # Check raceDetails first so we can use it for both race_link and
            # isRunningThisCycle. Some candidates (e.g. special-election winners
            # who haven't yet filed for the regular 2026 cycle) have stale FEC
            # election_years that don't include 2026 yet. If we can confirm they
            # appear in a regular (non-special) tracked race we mark them as
            # running this cycle regardless.
            race_doc = (
                db.client.collection("raceDetails").document(candidate["state"]).get()
            )
            race_data = race_doc.to_dict() if race_doc.exists else None
            in_regular_race = False
            if race_data:
                if candidate["office"] == "S":
                    if "S" in race_data and race_has_candidate(
                        race_data["S"], candidate_id
                    ):
                        in_regular_race = True
                        candidate_data[candidate_id][
                            "race_link"
                        ] = f"/elections/{candidate['state']}-S"
                    elif "S-special" in race_data and race_has_candidate(
                        race_data["S-special"], candidate_id
                    ):
                        candidate_data[candidate_id][
                            "race_link"
                        ] = f"/elections/{candidate['state']}-S-special"
                elif candidate["office"] == "H":
                    district = (
                        candidate["district"]
                        if candidate["state"] not in SINGLE_MEMBER_STATES
                        else "01"
                    )
                    if f"H-{district}" in race_data and race_has_candidate(
                        race_data[f"H-{district}"], candidate_id
                    ):
                        in_regular_race = True
                        candidate_data[candidate_id][
                            "race_link"
                        ] = f"/elections/{candidate['state']}-H-{district}"
                    elif f"H-{district}-special" in race_data and race_has_candidate(
                        race_data[f"H-{district}-special"], candidate_id
                    ):
                        candidate_data[candidate_id][
                            "race_link"
                        ] = f"/elections/{candidate['state']}-H-{district}-special"
            candidate_data[candidate_id]["isRunningThisCycle"] = (
                2026 in candidate["election_years"] or in_regular_race
            )

    # For candidates whose FEC election_years don't include 2026, check if
    # candidateAliases maps them to a canonical ID that IS running in 2026.
    # This handles cases like Deaton (S4MA00358 → S6MA00304) where the
    # committee's candidate_id is a stale filing whose FEC record predates
    # the 2026 cycle.
    alias_targets = {}  # canonical_id → original_id
    for cid in list(candidate_data.keys()):
        if candidate_data[cid].get("isRunningThisCycle"):
            continue
        canonical = db.candidate_aliases.get(cid)
        if canonical and canonical not in candidate_data:
            alias_targets[canonical] = cid

    if alias_targets:
        for ids_chunk in chunk(list(alias_targets.keys()), 10):
            data = FEC_fetch(
                session,
                "alias target candidates",
                "https://api.open.fec.gov/v1/candidates/",
                params={"candidate_id": ids_chunk},
            )
            if not data:
                continue
            for candidate in data["results"]:
                canonical_id = candidate["candidate_id"]
                if canonical_id not in alias_targets:
                    continue
                original_id = alias_targets[canonical_id]
                if 2026 in candidate["election_years"]:
                    candidate_data[original_id]["isRunningThisCycle"] = True
                    logging.info(
                        f"Alias {original_id} → {canonical_id}: setting isRunningThisCycle=True"
                    )

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

    # Backfill race_link for candidates already in candidate_details but missing it.
    # This handles cases where a committee was first processed before the candidate
    # appeared in raceDetails (e.g. they hadn't yet declared for this cycle).
    # Also clears any stale race_link values for non-congressional candidates (e.g.
    # presidential candidates who were incorrectly assigned a race_link in a prior
    # version of this script).
    cached_race_docs = {}
    for recipient_id in recipients.keys():
        for candidate_id, details in recipients[recipient_id].get(
            "candidate_details", {}
        ).items():
            if not details:
                continue
            office = details.get("office")
            if office not in {"S", "H"} and "race_link" in details:
                del details["race_link"]
            if "race_link" in details:
                continue
            state = details.get("state")
            if not state or not office or office not in {"S", "H"}:
                continue
            if state not in cached_race_docs:
                race_doc = (
                    db.client.collection("raceDetails").document(state).get()
                )
                cached_race_docs[state] = (
                    race_doc.to_dict() if race_doc.exists else None
                )
            race_data = cached_race_docs[state]
            if not race_data:
                continue
            # Also try the canonical alias in case the committee's candidate_id
            # is a stale filing that maps to the ID stored in raceDetails.
            canonical_id = db.candidate_aliases.get(candidate_id, candidate_id)
            if office == "S":
                if "S" in race_data and (
                    race_has_candidate(race_data["S"], candidate_id)
                    or race_has_candidate(race_data["S"], canonical_id)
                ):
                    details["race_link"] = f"/elections/{state}-S"
                elif "S-special" in race_data and (
                    race_has_candidate(race_data["S-special"], candidate_id)
                    or race_has_candidate(race_data["S-special"], canonical_id)
                ):
                    details["race_link"] = f"/elections/{state}-S-special"
            elif office == "H":
                district = details.get("district")
                if not district:
                    continue
                if state in SINGLE_MEMBER_STATES:
                    district = "01"
                if f"H-{district}" in race_data and (
                    race_has_candidate(race_data[f"H-{district}"], candidate_id)
                    or race_has_candidate(race_data[f"H-{district}"], canonical_id)
                ):
                    details["race_link"] = f"/elections/{state}-H-{district}"
                elif f"H-{district}-special" in race_data and (
                    race_has_candidate(
                        race_data[f"H-{district}-special"], candidate_id
                    )
                    or race_has_candidate(
                        race_data[f"H-{district}-special"], canonical_id
                    )
                ):
                    details["race_link"] = f"/elections/{state}-H-{district}-special"

    return recipients
