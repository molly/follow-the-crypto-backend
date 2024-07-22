import logging
import re
from states import SINGLE_MEMBER_STATES
from utils import chunk, FEC_fetch, pick


def all_unique(numbers):
    """
    Check if all integers in the array are unique.
    """
    return len(numbers) == len(set(numbers))


def get_description(contrib):
    desc = contrib.get("memo_text", "")
    if not desc:
        desc = contrib.get("receipt_type_full", "")
    if not desc:
        return ""
    return desc


def remove_efiled(contribs, remove_all=False):
    # If remove_all is true, this will remove all efiled contributions.
    # Otherwise, it will only remove an efiled contribution if there are only two options that are otherwise identical
    if (
        not remove_all
        and len(contribs) == 2
        and contribs[0]["contribution_receipt_amount"]
        == contribs[1]["contribution_receipt_amount"]
    ):
        # Identical contributions, but one was efiled. In this case, prefer the non-efiled one.
        if contribs[0].get("efiled") and not contribs[1].get("efiled"):
            return [contribs[1]]
        elif contribs[1].get("efiled") and not contribs[0].get("efiled"):
            return [contribs[0]]
    elif remove_all:
        return [c for c in contribs if not c.get("efiled")]
    return contribs


def diff(a, b):
    """
    Diff two dicts
    """
    return {k: b[k] for k in set(b) - set(a)}


def dedupe_by_memo(group):
    contribs_by_id = {}
    for contrib in group:
        contribs_by_id[contrib["transaction_id"]] = contrib

    for contrib in group:
        if contrib["transaction_id"] not in contribs_by_id:
            # Possible this was removed in an earlier pass
            continue
        description = get_description(contrib)
        if description:
            c_id_match = re.search(r"\((C\d+)\)", get_description(contrib))
            if c_id_match:
                c_id = c_id_match.group(1)
                contribs_to_id = [
                    x for x in contribs_by_id.values() if x.get("committee_id") == c_id
                ]
                if contribs_to_id and (
                    sum([x["contribution_receipt_amount"] for x in contribs_to_id])
                    == contrib["contribution_receipt_amount"]
                ):
                    # Remove the parent transaction
                    del contribs_by_id[contrib["transaction_id"]]
            else:
                transfer_match = re.search(r"FROM (.*?)(?:$| JFC)", description)
                if transfer_match:
                    from_source = [
                        x
                        for x in contribs_by_id.values()
                        if x["committee_name"] == transfer_match.group(1)
                    ]
                    if from_source:
                        from_source_sum = sum(
                            [x["contribution_receipt_amount"] for x in from_source]
                        )
                        to = [
                            x
                            for x in contribs_by_id.values()
                            if (
                                get_description(x)
                                and transfer_match.group(1) in get_description(x)
                            )
                        ]
                        to_sum = sum([x["contribution_receipt_amount"] for x in to])
                        if from_source_sum == to_sum:
                            # Remove the parent transaction
                            for from_contrib in from_source:
                                del contribs_by_id[from_contrib["transaction_id"]]
    return [attribute_earmarked(c) for c in list(contribs_by_id.values())]


def attribute_earmarked(contrib):
    # Try to attribute the contribution to a committee mentioned in the description
    description = get_description(contrib)
    if description:
        c_id = re.search(r"\((C\d+)\)", description)
        if c_id:
            return {
                **contrib,
                "committee_id": c_id.group(1),
                "committee_name": None,
                "candidate_ids": [],
                "committee_type": None,
                "committee_type_full": None,
                "designation": None,
                "designation_full": None,
                "party": None,
                "state": None,
            }
    return contrib


def dedupe_by_ids(group):
    grouped_by_transaction_id = {}
    for contrib in group:
        if contrib["transaction_id"] not in grouped_by_transaction_id:
            grouped_by_transaction_id[contrib["transaction_id"]] = []
        grouped_by_transaction_id[contrib["transaction_id"]].append(contrib)

    deduped_by_id = []
    for contribs in grouped_by_transaction_id.values():
        if len(contribs) == 1:
            deduped_by_id.append(contribs[0])
            continue
        deduped = remove_efiled(contribs)
        if len(deduped) == 1:
            deduped_by_id.append(deduped[0])
            continue

        # If the transaction has been amended, get the most recent. Hopefully that will leave us with just one.
        amendment_lengths = [len(x["amendment_chain"]) for x in contribs]
        longest = max(amendment_lengths)
        contribs = [x for x in contribs if len(x["amendment_chain"]) == longest]
        if len(contribs) == 1:
            deduped_by_id.append(contribs[0])
            continue
        else:
            # See if one of the contributions was an earmarked one, in which case it should be removed.
            reattributed = [
                x for x in contribs if "EARMARK" not in get_description(x).upper()
            ]
            if len(reattributed) == 0:
                pass
            elif len(reattributed) < len(contribs):
                contribs = remove_efiled(reattributed)
                if len(contribs) == 1:
                    deduped_by_id.append(contribs[0])
                    continue
            else:
                contribs = reattributed

        # Last ditch, remove all efiled contribs
        no_efiled = remove_efiled(contribs, True)
        if len(no_efiled) > 0:
            deduped_by_id.append(no_efiled[0])
            continue
        else:
            deduped_by_id.append(contribs[0])
            continue

    return deduped_by_id


def process_contribution_group(group):
    if len(group) == 1:
        return [attribute_earmarked(group[0])]
    group = dedupe_by_ids(group)
    if len(group) == 1:
        return [attribute_earmarked(group[0])]
    contrib_24t = []
    contrib_15 = []
    contrib_15e = []
    contrib_15x = []
    contrib_rest = []
    for contrib in group:
        receipt_type = contrib.get("receipt_type")
        if not receipt_type:
            contrib_rest.append(contrib)
        else:
            if receipt_type == "24T":
                contrib_24t.append(contrib)
            elif receipt_type == "15":
                contrib_15.append(contrib)
            elif receipt_type == "15E":
                contrib_15e.append(contrib)
            elif receipt_type.startswith("15"):
                contrib_15x.append(contrib)
            else:
                contrib_rest.append(contrib)

    sum_24t = sum([x["contribution_receipt_amount"] for x in contrib_24t])
    sum_15 = sum([x["contribution_receipt_amount"] for x in contrib_15])
    sum_15e = sum([x["contribution_receipt_amount"] for x in contrib_15e])
    sum_15x = sum([x["contribution_receipt_amount"] for x in contrib_15x])
    sum_rest = sum([x["contribution_receipt_amount"] for x in contrib_rest])

    contribs_to_keep = []

    if sum_24t == sum_15 == sum_15e == sum_15x == sum_rest == 0:
        totals = {}
        for contrib in group:
            if contrib["committee_id"] not in totals:
                totals[contrib["committee_id"]] = 0
            totals[contrib["committee_id"]] += contrib["contribution_receipt_amount"]
            if all([x == 0 for x in totals.values()]):
                # Just redesignation(s), can ignore
                return []
        else:
            print(f"Encountered an unexpected contributions group.")
            logging.error(
                f"Encountered an unexpected contributions group.", {"group": group}
            )

    if not contrib_24t and not contrib_15 and not contrib_15e:
        # No earmarked contributions, these are just multiple contributions on the same day
        return contrib_15x + contrib_rest

    if contrib_24t:
        if sum_24t != sum_15 and sum_24t != sum_15e and sum_24t != sum_15x:
            contribs_to_keep.extend(dedupe_by_memo(group))
            return contribs_to_keep
        else:
            # Drop 24t contribution, fall through to 15 handling
            pass

    if contrib_15 or contrib_15e or contrib_15x:
        if sum_15e == 0 and sum_15 > 0 and sum_15 == sum_15x:
            contribs_to_keep.extend(contrib_15x + contrib_rest)
        elif sum_15 == 0 and sum_15e > 0 and sum_15e == sum_15x:
            contribs_to_keep.extend(contrib_15x + contrib_rest)
        elif not contrib_15x:
            if not contrib_15e:
                contribs_to_keep.extend(contrib_15 + contrib_rest)
            elif not contrib_15:
                contribs_to_keep.extend(contrib_15e + contrib_rest)
            else:
                contribs_to_keep.extend(dedupe_by_memo(group))
                return contribs_to_keep
        else:
            contribs_to_keep.extend(dedupe_by_memo(group))
            return contribs_to_keep
    else:
        print(f"Encountered an unexpected contributions group.")
        logging.error(
            f"Encountered an unexpected contributions group.", {"group": group}
        )

    return contribs_to_keep


def get_missing_recipient_data(recipients, db):
    committee_data = {}

    needs_data_ids = [k for k, v in recipients.items() if v.get("needs_data", False)]
    for ids_chunk in chunk(needs_data_ids, 10):
        data = FEC_fetch(
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
                    candidate_id not in recipients[recipient_id]["candidate_details"]
                    and candidate_id not in candidate_data
                ):
                    candidate_data[candidate_id] = {}
        if recipients[recipient_id].get("sponsor_candidate_ids") is not None:
            for candidate_id in recipients[recipient_id]["sponsor_candidate_ids"]:
                if (
                    candidate_id not in recipients[recipient_id]["candidate_details"]
                    and candidate_id not in candidate_data
                ):
                    candidate_data[candidate_id] = {}

    for ids_chunk in chunk(list(candidate_data.keys()), 10):
        data = FEC_fetch(
            "candidate",
            "https://api.open.fec.gov/v1/candidates/",
            params={"candidate_id": ids_chunk},
        )
        for candidate in data["results"]:
            candidate_id = candidate["candidate_id"]
            candidate_data[candidate_id] = pick(
                candidate, ["name", "party", "state", "office", "district"]
            )
            race_data = (
                db.client.collection("raceDetails")
                .document(candidate["state"])
                .get()
                .to_dict()
            )
            if race_data:
                if candidate["office"] == "S" and "S" in race_data:
                    candidate_data[candidate_id][
                        "race_link"
                    ] = f"/races/{candidate['state']}-S"
                elif candidate["office"] == "H":
                    district = (
                        candidate["district"]
                        if candidate["state"] not in SINGLE_MEMBER_STATES
                        else "01"
                    )
                    if f"H-{district}" in race_data:
                        candidate_data[candidate_id][
                            "race_link"
                        ] = f"/races/{candidate['state']}-H-{district}"

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


def process_individual_contributions(db):
    all_recipients = (
        db.client.collection("allRecipients").document("recipients").get().to_dict()
    )
    new_recipients = set()

    for doc in db.client.collection("rawIndividualContributions").stream():
        ind_id, ind = doc.id, doc.to_dict()
        contributions = ind["contributions"]

        grouped_by_date = {}
        for contrib in contributions:
            if contrib["contribution_receipt_date"] not in grouped_by_date:
                grouped_by_date[contrib["contribution_receipt_date"]] = []
            grouped_by_date[contrib["contribution_receipt_date"]].append(contrib)

        deduped = []
        grouped_by_recipient = {}
        for date, contribs in grouped_by_date.items():
            deduped.extend(process_contribution_group(contribs))

        for contrib in deduped:
            recipient = contrib["committee_id"]
            if recipient not in grouped_by_recipient:
                grouped_by_recipient[recipient] = {
                    "contributions": [],
                    "total": 0,
                    "committee_id": recipient,
                }
            if recipient not in all_recipients:
                new_recipients.add(recipient)
                all_recipients[recipient] = {
                    "committee_id": recipient,
                    "candidate_details": {},
                    "needs_data": True,
                }
            grouped_by_recipient[recipient]["contributions"].append(contrib)
            grouped_by_recipient[recipient]["total"] += contrib[
                "contribution_receipt_amount"
            ]

        if "claimedContributions" in db.individuals[ind_id]:
            for claimed_contrib in db.individuals[ind_id]["claimedContributions"]:
                c_id = claimed_contrib["committee_id"]
                if c_id in grouped_by_recipient:
                    if any(
                        [
                            "claimed" not in x
                            for x in grouped_by_recipient[c_id]["contributions"]
                        ]
                    ):
                        logging.warning(
                            "Claimed contribution committee also appears in FEC data, check for duplicates.",
                            {"ind_id": ind_id, "claimed_contrib": claimed_contrib},
                        )
                        print(
                            "Claimed contribution committee also appears in FEC data, check for duplicates. Ind: {} Committee: {}".format(
                                ind_id, claimed_contrib["committee_id"]
                            )
                        )
                    grouped_by_recipient[c_id]["total"] += claimed_contrib[
                        "contribution_receipt_amount"
                    ]
                    grouped_by_recipient[c_id]["contributions"].append(
                        {**claimed_contrib, "claimed": True, "committee_id": c_id}
                    )
                else:
                    grouped_by_recipient[c_id] = {
                        "contributions": [
                            {**claimed_contrib, "claimed": True, "committee_id": c_id}
                        ],
                        "committee_id": c_id,
                        "total": claimed_contrib["contribution_receipt_amount"],
                    }
                if c_id not in all_recipients:
                    all_recipients[c_id] = {
                        "committee_id": c_id,
                        "needs_data": True,
                    }
        all_contribs = deduped + db.individuals.get(ind_id, {}).get(
            "claimedContributions", []
        )
        by_date = sorted(
            all_contribs,
            key=lambda x: x["contribution_receipt_date"],
            reverse=True,
        )
        sorted_contributions = sorted(
            grouped_by_recipient.values(), key=lambda x: x["total"], reverse=True
        )
        db.client.collection("individuals").document(ind_id).set(
            {
                **ind,
                "contributions": sorted_contributions,
                "contributions_by_date": by_date,
            }
        )

    # Get recipient data and record any new committees
    recipients = get_missing_recipient_data(all_recipients, db)
    db.client.collection("allRecipients").document("recipients").set(recipients)

    # Summarize spending by party
    # Sadly can't do this in the first loop because it relies on data from get_missing_recipient_data
    for doc in db.client.collection("individuals").stream():
        ind_id, ind = doc.id, doc.to_dict()
        contributions = ind["contributions"]
        party_summary = {}
        for group_data in contributions:
            committee_id = group_data.get("committee_id", None)
            party = "UNK"
            if committee_id in recipients:
                committee = recipients[committee_id]
                if (
                    "party" in committee
                    and committee["party"] is not None
                    and not committee["party"].startswith("N")
                ):
                    party = committee["party"]
                else:
                    parties = [
                        c.get("party")
                        for c in committee["candidate_details"].values()
                        if c.get("party") is not None
                    ]
                    if len(set(parties)) == 1 and not parties[0].startswith("N"):
                        party = parties[0]
                if party not in party_summary:
                    party_summary[party] = 0
            party_summary[party] += group_data["total"]

        db.client.collection("individuals").document(ind_id).set(
            {"party_summary": party_summary}, merge=True
        )

    return new_recipients
