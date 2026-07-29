import logging
import re
from utils import FEC_fetch, compare_names, get_expenditure_race_type, get_sector_keys
from states import SINGLE_MEMBER_STATES
from unidecode import unidecode
from race_utils import get_all_races, update_race
from recipient_utils import has_significant_direct_support

def trim_name(name):
    m = re.match(r"^(.+)(\s(?:[SJ]r\.?|IX|IV|V?I{0,3}))$", name)
    if m:
        last_name = m.group(1).split(" ")[-1]
    else:
        last_name = unidecode(name.split(" ")[-1])
    if len(last_name) < 3:
        # FEC API won't accept queries of < 3 characters, so short names like "Xu" throw errors
        return unidecode(name)
    return unidecode(last_name)


def summarize_races(db, session):
    all_race_data = get_all_races(db.client)
    all_expenditures = (
        db.client.collection("expenditures").document("all").get().to_dict()
    )
    states_expenditures = (
        db.client.collection("expenditures").document("states").get().to_dict()
    )
    recipient_docs = db.client.collection("recipientDetails").stream()
    recipients = {doc.id: doc.to_dict() for doc in recipient_docs}
    for state, state_data in all_race_data.items():
        races_expenditures = states_expenditures.get(state, {}).get("by_race", {})
        # Iterate through each race in each state
        for race_id, race_data in state_data.items():
            race_id_split = race_id.split("-")
            full_race_id = "{}-{}".format(state, race_id)
            race_expenditures = races_expenditures.get(full_race_id, {}).get(
                "expenditures", []
            )

            # Deduplicate candidates in each sub-race (cleanup from previous bug).
            # Prefer non-withdrawn versions of candidates.
            for race in race_data.get("races", []):
                candidates_by_name = {}
                for candidate in race.get("candidates", []):
                    name = candidate.get("name")
                    if name is None:
                        logging.warning(
                            f"Candidate missing 'name' field in {state} {race_id}, skipping: {candidate}"
                        )
                        continue
                    if name not in candidates_by_name:
                        candidates_by_name[name] = candidate
                    elif (
                        "withdrew_race" in candidates_by_name[name]
                        and "withdrew_race" not in candidate
                    ):
                        # Replace withdrawn version with non-withdrawn version
                        candidates_by_name[name] = candidate
                race["candidates"] = list(candidates_by_name.values())

            # Get total spending for each candidate, by committee, by race
            spending = {}

            # Create set for each unique candidate in any sub-race in this race. This will always be equivalent to
            # Object.keys(candidates_data) and is just maintained for convenience.
            #
            # Placeholder entries are excluded throughout: they stand in for a
            # nominee who hasn't been named yet, so there is no person to
            # summarize, no FEC record to look up, and no money to attribute.
            # Leaving them in would send their slot name ("Democratic candidate
            # TBD") to the FEC candidates/search endpoint, where compare_names
            # could fuzzy-match it onto an unrelated filer.
            try:
                candidates = {
                    candidate["name"]
                    for race in race_data["races"]
                    for candidate in race["candidates"]
                    if not candidate.get("placeholder")
                }
            except KeyError as e:
                logging.error(f"Missing race data for {state} {race_id}: {e}")
                continue
            # Create dict with an entry for each candidate. This dict will eventually be saved to the "candidates" field
            # in the race entry.
            candidates_data = {
                candidate: {
                    "common_name": candidate,
                    "support_total": 0,
                    "oppose_total": 0,
                    "crypto_support_total": 0,
                    "ai_support_total": 0,
                    "crypto_oppose_total": 0,
                    "ai_oppose_total": 0,
                    "races": [],  # Sub-races in which this person was a candidate
                }
                for candidate in candidates
            }

            # Add withdrawn candidates to this set and dict
            if "withdrew" in race_data:
                withdrawn_candidates = {
                    candidate for candidate in race_data["withdrew"].keys()
                }
                candidates = candidates.union(withdrawn_candidates)
                for candidate in withdrawn_candidates:
                    candidates_data[candidate] = {
                        "common_name": candidate,
                        "support_total": 0,
                        "oppose_total": 0,
                        "crypto_support_total": 0,
                        "ai_support_total": 0,
                        "crypto_oppose_total": 0,
                        "ai_oppose_total": 0,
                        "races": [],
                        "withdrew": True,
                        "withdrew_race": None,  # Race from which candidate withdrew
                    }

            # Try to get candidate data from FEC.
            # The stored "year" can be an odd year for special elections
            # (e.g. 2025 for a January 2025 special).  The FEC candidates/search
            # endpoint only accepts even cycle years, so round up to the next
            # even year when necessary.
            stored_year = race_data.get("year", 2026)
            election_year = stored_year if stored_year % 2 == 0 else stored_year + 1
            params = {
                "office": race_id_split[0],
                "state": state,
                "election_year": election_year,
                "q": map(trim_name, candidates),
                "per_page": 50,
            }
            if (
                race_id_split[0] == "H"
                and len(race_id_split) > 1
                and state not in SINGLE_MEMBER_STATES
            ):
                params["district"] = race_id_split[1]

            FEC_candidates_data = FEC_fetch(
                session,
                f"candidates data for {state}",
                "https://api.open.fec.gov/v1/candidates/search",
                params,
            )

            # Map FEC candidate names to formatted candidate names (which are being used as keys)
            names = {}
            # Per matched candidate: whether the FEC result we assigned agreed on
            # the first name, and which FEC name it was. The surname search returns
            # everyone of that surname who ever filed in the district, so when a
            # candidate has a single same-surname match in our set we must still
            # prefer the first-name match — otherwise a different person sharing the
            # surname (e.g. Jimih Jones, returned alongside the Eric Jones actually
            # running in CA-04) clobbers the correct match on last-write-wins.
            matched_first_name = {}
            assigned_fec_name = {}
            # Add relevant FEC data to candidate data
            for FEC_candidate_data in FEC_candidates_data["results"]:
                # Try to match FEC candidate result to candidate in our data
                split_name = FEC_candidate_data["name"].split(", ")
                last_name = split_name[0]
                first_name = split_name[1].split(" ")[0] if len(split_name) > 1 else ""

                # Get the common name for this candidate
                candidate_race_name = None
                candidate = {
                    name for name in candidates if compare_names(last_name, name)
                }
                if len(candidate) == 1:
                    candidate_race_name = candidate.pop()
                elif len(candidate) > 1:
                    # Sometimes there are multiple candidates with the same surname, in which case we compare first
                    # names.
                    candidate = {
                        name for name in candidate if compare_names(first_name, name)
                    }
                    if len(candidate) == 1:
                        candidate_race_name = candidate.pop()
                if candidate_race_name is None:
                    # There weren't any matching candidates, or there were still multiple candidates in the results
                    # This is not ALWAYS an error — there can be results from the FEC API for candidates we're not
                    # interested in (and who therefore aren't represented in the candidates set).
                    logging.debug(
                        f"Having trouble locating FEC candidate in candidates data: {first_name} {last_name} in {state} {race_id}"
                    )
                    continue

                # When a candidate has a single same-surname match, the surname
                # check above can't tell two people apart, so verify the first name
                # before letting this result overwrite an existing one. A result
                # whose first name matches always wins over one that doesn't; among
                # equally-(un)matched results, last-write-wins as before.
                first_name_matches = bool(
                    first_name and compare_names(first_name, candidate_race_name)
                )
                if (
                    candidates_data[candidate_race_name].get("candidate_id") is not None
                    and matched_first_name.get(candidate_race_name)
                    and not first_name_matches
                ):
                    continue
                # If this result supersedes a prior, worse match, drop the prior
                # FEC name's stale mapping so it can't misroute an expenditure.
                prev_fec_name = assigned_fec_name.get(candidate_race_name)
                if prev_fec_name is not None and prev_fec_name in names:
                    del names[prev_fec_name]
                matched_first_name[candidate_race_name] = first_name_matches
                assigned_fec_name[candidate_race_name] = FEC_candidate_data["name"]

                # Map FEC name to common name
                names[FEC_candidate_data["name"]] = candidate_race_name

                # Add FEC data to candidate data map
                candidates_data[candidate_race_name][
                    "candidate_id"
                ] = FEC_candidate_data["candidate_id"]
                if FEC_candidate_data["party"]:
                    candidates_data[candidate_race_name]["party"] = FEC_candidate_data[
                        "party"
                    ][0]
                candidates_data[candidate_race_name][
                    "incumbent_challenge"
                ] = FEC_candidate_data["incumbent_challenge"]
                candidates_data[candidate_race_name]["FEC_name"] = FEC_candidate_data[
                    "name"
                ]

            for entry in candidates_data.values():
                if "candidate_id" not in entry:
                    if entry["common_name"] and entry["common_name"] in db.candidates:
                        # A few weird edge cases are in the candidates constant, get that data here
                        c_id = db.candidates[entry["common_name"]]
                        # Normalize through candidateAliases so that a stale House ID
                        # (e.g. H8WY00148) resolves to the canonical Senate ID
                        # (e.g. S0WY00137) and matches what allRecipients stores.
                        c_id = db.candidate_aliases.get(c_id, c_id)
                        FEC_candidates_data = FEC_fetch(
                            session,
                            f"candidates data for {state}",
                            "https://api.open.fec.gov/v1/candidates/search",
                            {"candidate_id": [c_id]},
                        )
                        if not FEC_candidates_data["results"]:
                            continue
                        FEC_candidate_data = FEC_candidates_data["results"][0]
                        names[FEC_candidate_data["name"]] = entry["common_name"]
                        candidates_data[entry["common_name"]][
                            "candidate_id"
                        ] = FEC_candidate_data["candidate_id"]

                        candidates_data[entry["common_name"]]["party"] = (
                            FEC_candidate_data["party"][0],
                        )

                        candidates_data[entry["common_name"]][
                            "incumbent_challenge"
                        ] = FEC_candidate_data["incumbent_challenge"]
                        candidates_data[entry["common_name"]][
                            "FEC_name"
                        ] = FEC_candidate_data["name"]
                    else:
                        logging.debug(
                            f"Having trouble locating FEC candidate: {entry['common_name']} in {state}-{race_id}"
                        )

                if candidates_data[entry["common_name"]].get("candidate_id"):
                    candidate_id = candidates_data[entry["common_name"]]["candidate_id"]
                    # Normalize through candidate aliases so stale IDs (e.g. old House
                    # ID after switching to Senate) resolve to the canonical ID used as
                    # the recipientDetails document key.
                    candidate_id = db.candidate_aliases.get(candidate_id, candidate_id)
                    recipient = recipients.get(candidate_id)
                    # Only count as tracked support when it clears the significance
                    # bar ($25k total or a $10k single contributor). A bare
                    # recipientDetails entry can hold token amounts that shouldn't,
                    # on their own, keep an otherwise-inactive race tracked.
                    if recipient and has_significant_direct_support(recipient):
                        candidates_data[entry["common_name"]][
                            "has_non_pac_support"
                        ] = True

            # Iterate through each subrace
            for race in race_data["races"]:
                # Iterate through each candidate in the subrace. These should generally be in reverse chrono order.
                for candidate in race["candidates"]:
                    if candidate.get("placeholder"):
                        # No summary entry exists for a placeholder, so there is
                        # nothing to attach a subrace or party to.
                        continue
                    # Add this subrace to their list of involved races
                    race_type = race.get("type")
                    if race_type is None:
                        logging.warning(
                            f"Race missing 'type' field in {state} {race_id}: {race}"
                        )
                    candidates_data[candidate["name"]]["races"].append(race_type)
                    # Use party from race data as a fallback for candidates FEC didn't
                    # find (e.g., incumbents who declined to run in 2026 and therefore
                    # don't appear in the FEC candidates/search results for this cycle).
                    if (
                        "party" not in candidates_data[candidate["name"]]
                        and "party" in candidate
                    ):
                        candidates_data[candidate["name"]]["party"] = candidate["party"]
                    # Win/loss is intentionally not stored on the summary: the
                    # frontend derives it from the per-race `won` flags (see
                    # isDefeated / getMostRecentRaceResult), which stay correct
                    # when races are edited manually between summarize runs.
                    if "declined" in candidate and candidate["declined"] is True:
                        candidates_data[candidate["name"]]["declined"] = True
                        if "declinedReason" in candidate:
                            candidates_data[candidate["name"]][
                                "declinedReason"
                            ] = candidate["declinedReason"]
                    if "declared" in candidate and candidate["declared"] is False:
                        candidates_data[candidate["name"]]["declared"] = False
                    # `died` is set by hand in the race editor, so unlike
                    # `withdrew` it only ever lives on the race candidate. Lift it
                    # onto the summary so Outcome/Spending can read it without
                    # walking the races.
                    if candidate.get("died") is True:
                        candidates_data[candidate["name"]]["died"] = True

            # Map FEC candidate_id -> common name for expenditures whose name
            # strings don't line up with the FEC candidate record (e.g. compound
            # surnames: efile reports "McClain Delaney" while the FEC record
            # orders it "Delaney, April McClain"). candidate_id is normalized
            # through candidate_aliases on both sides, so it's the stable key.
            candidate_key_by_id = {
                cand["candidate_id"]: key
                for key, cand in candidates_data.items()
                if cand.get("candidate_id")
            }

            # Iterate through each expenditure in this race
            for expenditure_id in race_expenditures:
                expenditure = all_expenditures[expenditure_id]
                if not expenditure["expenditure_amount"]:
                    continue

                # Try to find the candidate this expenditure is associated with
                try:
                    # Ideally this will match their FEC_name
                    candidate_key = names[expenditure["candidate_name"]]
                except KeyError:
                    # Prefer an exact candidate_id match before falling back to
                    # fuzzy name matching — it's robust to name-format quirks
                    # like compound surnames that the substring check below misses.
                    candidate_key = candidate_key_by_id.get(
                        expenditure.get("candidate_id")
                    )
                    if candidate_key is None:
                        # Otherwise, try to find the candidate with a matching last name
                        k = None
                        ks = {
                            key
                            for key in names
                            if expenditure["candidate_last_name"].upper() in key
                        }
                        if len(ks) == 1:
                            k = ks.pop()
                        elif len(ks) > 1:
                            # If there are multiple candidates with the same last name, try to narrow down by first name
                            k = None
                            filtered = [
                                k
                                for k in ks
                                if expenditure["candidate_first_name"].upper() in k
                            ]
                            if len(filtered) == 1:
                                k = filtered[0]
                        if k is None:
                            # TODO: We're going to have to figure out something else if we end up here.
                            logging.error(
                                f"Having trouble locating candidate named in expenditure: {expenditure['candidate_name']} in {state} {race_id}"
                            )
                            continue
                        else:
                            candidate_key = names[k]

                # Initialize fields if necessary
                if "expenditure_races" not in candidates_data[candidate_key]:
                    candidates_data[candidate_key]["expenditure_races"] = set()
                if "expenditure_committees" not in candidates_data[candidate_key]:
                    candidates_data[candidate_key]["expenditure_committees"] = set()

                # Add the expenditure's sub-race to the candidate's list of expenditure_races.
                stored_subrace = expenditure.get("subrace", None)

                # Sort races descending by date so get_expenditure_race_type() finds the nearest
                # future race correctly regardless of how races were manually ordered in Firestore.
                sorted_races = sorted(
                    race_data.get("races", []),
                    key=lambda r: r.get("date") or "",
                    reverse=True,
                )

                if expenditure.get("election_type"):
                    # election_type is available: recompute deterministically from the FEC code.
                    # The races list is only consulted for "O" (Other) codes, which no race can
                    # match and which therefore have to be resolved by date.
                    subrace = get_expenditure_race_type(expenditure, sorted_races)
                    if not subrace:
                        subrace = stored_subrace
                    elif subrace != stored_subrace:
                        db.client.collection("expenditures").document("all").update(
                            {db.client.field_path(expenditure_id, "subrace"): subrace}
                        )
                else:
                    # Efiled expenditure: no election_type, trust the stored subrace.
                    subrace = stored_subrace
                    if subrace is None:
                        subrace = get_expenditure_race_type(expenditure, sorted_races)
                        if subrace:
                            db.client.collection("expenditures").document("all").update(
                                {db.client.field_path(expenditure_id, "subrace"): subrace}
                            )
                if subrace:
                    candidates_data[candidate_key]["expenditure_races"].add(subrace)

                c_id = expenditure["committee_id"]

                # Add expenditure to total support/oppose amount
                if expenditure["support_oppose_indicator"] == "S":
                    candidates_data[candidate_key]["support_total"] = round(
                        candidates_data[candidate_key]["support_total"]
                        + expenditure["expenditure_amount"],
                        2,
                    )
                    sector_keys = get_sector_keys(db.committees.get(c_id, {}).get("sector"))
                    if "crypto" in sector_keys:
                        candidates_data[candidate_key]["crypto_support_total"] = round(
                            candidates_data[candidate_key]["crypto_support_total"]
                            + expenditure["expenditure_amount"],
                            2,
                        )
                    if "ai" in sector_keys:
                        candidates_data[candidate_key]["ai_support_total"] = round(
                            candidates_data[candidate_key]["ai_support_total"]
                            + expenditure["expenditure_amount"],
                            2,
                        )
                elif expenditure["support_oppose_indicator"] == "O":
                    candidates_data[candidate_key]["oppose_total"] = round(
                        candidates_data[candidate_key]["oppose_total"]
                        + expenditure["expenditure_amount"],
                        2,
                    )
                    sector_keys = get_sector_keys(db.committees.get(c_id, {}).get("sector"))
                    if "crypto" in sector_keys:
                        candidates_data[candidate_key]["crypto_oppose_total"] = round(
                            candidates_data[candidate_key]["crypto_oppose_total"]
                            + expenditure["expenditure_amount"],
                            2,
                        )
                    if "ai" in sector_keys:
                        candidates_data[candidate_key]["ai_oppose_total"] = round(
                            candidates_data[candidate_key]["ai_oppose_total"]
                            + expenditure["expenditure_amount"],
                            2,
                        )

                # Add expenditure to per-committee spending
                if c_id not in spending:
                    spending[c_id] = {"total": 0, "subraces": {}}
                spending[c_id]["total"] += expenditure["expenditure_amount"]
                if subrace:
                    if subrace not in spending[c_id]["subraces"]:
                        spending[c_id]["subraces"][subrace] = {"candidates": {}, "total": 0}
                    spending[c_id]["subraces"][subrace]["total"] += expenditure[
                        "expenditure_amount"
                    ]
                    if (
                        candidate_key
                        not in spending[c_id]["subraces"][subrace]["candidates"]
                    ):
                        spending[c_id]["subraces"][subrace]["candidates"][candidate_key] = {
                            "support": 0,
                            "oppose": 0,
                        }
                    if expenditure["support_oppose_indicator"] == "S":
                        spending[c_id]["subraces"][subrace]["candidates"][candidate_key][
                            "support"
                        ] += expenditure["expenditure_amount"]
                    elif expenditure["support_oppose_indicator"] == "O":
                        spending[c_id]["subraces"][subrace]["candidates"][candidate_key][
                            "oppose"
                        ] += expenditure["expenditure_amount"]

            # Handle candidates who have withdrawn
            withdrawn_candidates = (
                list(race_data["withdrew"].keys()) if "withdrew" in race_data else []
            )
            for candidate in withdrawn_candidates:
                if (
                    candidates_data[candidate]["support_total"] == 0
                    and candidates_data[candidate]["oppose_total"] == 0
                    and not candidates_data[candidate].get("declined")
                ):
                    # There can be a lot of withdrawn candidates, so only keep those involved in some expenditure or who declined
                    del candidates_data[candidate]
                    del race_data["withdrew"][candidate]
                else:
                    # However, if they were involved, we need to add them to the list of candidates in the race from
                    # which they withdrew.
                    if "withdrew_race" in race_data["withdrew"][candidate]:
                        candidate_details = race_data["withdrew"][candidate]
                        matching = next(
                            (
                                i
                                for i, race in enumerate(race_data["races"])
                                if (
                                    (
                                        race["type"]
                                        == candidate_details["withdrew_race"]["type"]
                                    )
                                    and (
                                        "party" in race
                                        and "party"
                                        in candidate_details["withdrew_race"]
                                        and race["party"]
                                        == candidate_details["withdrew_race"]["party"]
                                    )
                                )
                            ),
                            None,
                        )
                        if matching is not None:
                            # Only append if not already in the candidate list
                            existing_names = {
                                c["name"]
                                for c in race_data["races"][matching]["candidates"]
                            }
                            if candidate_details["name"] not in existing_names:
                                race_data["races"][matching]["candidates"].append(
                                    candidate_details
                                )

            # Get total raised for each candidate
            candidate_ids = [
                c["candidate_id"]
                for c in candidates_data.values()
                if "candidate_id" in c
            ]
            # FEC cycles are always even years. If the election_year is odd
            # (e.g. a 2025 special election), round up to the next even year.
            fec_cycle = election_year if election_year % 2 == 0 else election_year + 1
            FEC_totals_data = FEC_fetch(
                session,
                "candidate totals",
                "https://api.open.fec.gov/v1/candidates/totals",
                {
                    "cycle": fec_cycle,
                    "per_page": 50,
                    "candidate_id": candidate_ids,
                },
            )
            for total_result in FEC_totals_data["results"]:
                FEC_name = total_result["name"]
                if FEC_name not in names:
                    # This candidate was not found in the list of candidates for this
                    continue
                candidate_key = names[FEC_name]
                candidates_data[candidate_key]["raised_total"] = total_result[
                    "receipts"
                ]
                candidates_data[candidate_key]["spent_total"] = total_result[
                    "disbursements"
                ]

            updated_data = {
                db.client.field_path(race_id, "candidates"): candidates_data,
                db.client.field_path(race_id, "spending"): spending,
            }
            if "withdrew" in race_data:
                updated_data[db.client.field_path(race_id, "withdrew")] = race_data[
                    "withdrew"
                ]
                updated_data[db.client.field_path(race_id, "races")] = race_data[
                    "races"
                ]
            update_race(db.client, state, race_id, updated_data)
