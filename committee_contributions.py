import re
from utils import FEC_fetch, pick

SHARED_CONTRIBUTION_FIELDS = [
    "contributor_first_name",
    "contributor_middle_name",
    "contributor_last_name",
    "contributor_suffix",
    "contributor_name",
    "contributor_occupation",
    "contributor_employer",
    "entity_type",
    "contributor_aggregate_ytd",
    "redacted",
]

CONTRIBUTION_FIELDS = SHARED_CONTRIBUTION_FIELDS + [
    "contribution_receipt_amount",
    "contribution_receipt_date",
    "pdf_url",
    "receipt_type",
    "receipt_type_full",
    "transaction_id",
]

ROLLUP_CONTRIBUTION_FIELDS = [
    "oldest",
    "newest",
    "total",
    "total_receipt_amount",
]

ROLLUP_THRESHOLD = 10000


def pick_contribution_and_redact(d, keys, redacted):
    res = pick(d, keys)
    if redacted:
        for k in CONTRIBUTION_FIELDS[0:5]:
            res[k] = "REDACTED"
    res["redacted"] = redacted
    return res


def is_redacted(contrib, allowlists):
    """Redact any names for occupations not captured within the occupationAllowlist."""
    if not contrib["contributor_first_name"] and not contrib["contributor_last_name"]:
        # No redactions needed if this isn't an individual
        return False
    if not contrib["contributor_occupation"]:
        # Redact if the contributor is missing, just in case
        return True
    return not contrib["contributor_occupation"] in allowlists[
        "equals"
    ] and not allowlists["contains"].search(contrib["contributor_occupation"])


def should_omit(contrib, other_contribs, ids_to_omit):
    """Omit any duplicate contributions, refunds, etc."""
    if contrib["line_number"] in ["15", "16", "17"]:
        return True
    if contrib["transaction_id"] in ids_to_omit:
        # Manually excluded transaction, or a parent of a more granularly reported transaction
        return True
    if contrib["transaction_id"] in other_contribs:
        # Duplicate of a transaction we've already encountered
        return True
    if contrib["memo_text"] and "ATTRIBUTION" in contrib["memo_text"]:
        return True
    return False


def get_ids_to_omit(contribs):
    """Dedupe contributions, refunds, etc."""
    to_omit = set()
    transaction_ids = set([x["transaction_id"] for x in contribs])
    for t_id in transaction_ids:
        # There are sometimes 2+ transactions with IDs like SA17.4457 and SA17.4457.0, in which case we omit the former.
        # These are typically instances in which the committee has reported the dollar equivalent and the in-kind
        # contribution separately for the same contribution, or where a transaction from multiple people has been
        # reported as a group and then individually.
        m = re.match(r"^(.*?)\.\d$", t_id)
        if m:
            if m.group(1) in transaction_ids:
                to_omit.add(m.group(1))
    return to_omit


def update_committee_contributions(db):
    committee_ids = [committee["id"] for committee in db.committees.values()]
    for committee_id in committee_ids:
        donorMap = {
            "contributions_count": 0,
            "groups": {},
            "total_contributed": 0,
            "total_transferred": 0,
        }

        last_index = None
        last_contribution_receipt_amount = None
        contribs_count = 0
        redacted_count = 0
        contrib_ids = set()
        ids_to_omit = (
            set(db.duplicate_contributions[committee_id])
            if committee_id in db.duplicate_contributions
            else set()
        )
        while True:
            data = FEC_fetch(
                "committee contributions",
                "https://api.open.fec.gov/v1/schedules/schedule_a",
                params={
                    "committee_id": committee_id,
                    "two_year_transaction_period": [2024],
                    "per_page": 100,
                    "sort": "-contribution_receipt_amount",
                    "last_index": last_index,
                    "last_contribution_receipt_amount": last_contribution_receipt_amount,
                },
            )

            if not data:
                continue

            contribs_count += data["pagination"]["per_page"]

            results = data["results"]
            ids_to_omit = ids_to_omit.union(get_ids_to_omit(results))
            # TODO Edge case with duplicates that exist across pages
            for contrib in results:
                if should_omit(contrib, contrib_ids, ids_to_omit):
                    continue

                contrib_ids.add(contrib["transaction_id"])
                redacted = is_redacted(contrib, db.occupation_allowlist)

                # Get group name
                group = contrib["contributor_employer"] or contrib["contributor_name"]
                if group and group in db.individual_employers:
                    if redacted:
                        group = "REDACTED" + str(redacted_count)
                        redacted = True
                        redacted_count += 1
                    else:
                        group = contrib["contributor_name"]
                elif not group:
                    group = "UNKNOWN"
                elif group in db.company_aliases:
                    group = db.company_aliases[group]

                # Get the details to retain in our DB
                details = pick_contribution_and_redact(
                    contrib, CONTRIBUTION_FIELDS, redacted
                )

                # Add group to map if the group isn't already in there
                if group not in donorMap["groups"]:
                    donorMap["groups"][group] = {
                        "contributions": [],
                        "rollup": {},
                        "total": 0,
                    }

                if contrib["line_number"] == "12" or contrib["line_number"] == "11c":
                    # This was a transfer from another committee and shouldn't be double counted
                    donorMap["total_transferred"] = round(
                        donorMap["total_transferred"]
                        + contrib["contribution_receipt_amount"],
                        2,
                    )
                else:
                    donorMap["total_contributed"] = round(
                        donorMap["total_contributed"]
                        + contrib["contribution_receipt_amount"],
                        2,
                    )

                if details["contribution_receipt_amount"] >= ROLLUP_THRESHOLD:
                    # Record the individual contribution if it's large
                    donorMap["groups"][group]["contributions"].append(details)
                else:
                    # Add the contribution to a rollup.
                    # Note we don't redact here, that happens later
                    if (
                        contrib["contributor_name"]
                        not in donorMap["groups"][group]["rollup"]
                    ):
                        # Initialize the rollup group
                        donorMap["groups"][group]["rollup"][
                            contrib["contributor_name"]
                        ] = {
                            **pick(contrib, CONTRIBUTION_FIELDS),
                            "oldest": contrib["contribution_receipt_date"],
                            "newest": contrib["contribution_receipt_date"],
                            "total": 1,
                            "total_receipt_amount": round(
                                contrib["contribution_receipt_amount"], 2
                            ),
                            "redacted": redacted,
                        }
                    else:
                        donorMap["groups"][group]["rollup"][
                            contrib["contributor_name"]
                        ]["total"] += 1
                        donorMap["groups"][group]["rollup"][
                            contrib["contributor_name"]
                        ]["total_receipt_amount"] += round(
                            contrib["contribution_receipt_amount"], 2
                        )

                        # Set newest/oldest dates
                        if (
                            contrib["contribution_receipt_date"]
                            < donorMap["groups"][group]["rollup"][
                                contrib["contributor_name"]
                            ]["oldest"]
                        ):
                            donorMap["groups"][group]["rollup"][
                                contrib["contributor_name"]
                            ]["oldest"] = contrib["contribution_receipt_date"]
                        if (
                            contrib["contribution_receipt_date"]
                            > donorMap["groups"][group]["rollup"][
                                contrib["contributor_name"]
                            ]["newest"]
                        ):
                            donorMap["groups"][group]["rollup"][
                                contrib["contributor_name"]
                            ]["newest"] = contrib["contribution_receipt_date"]

                        # Update the aggregate YTD contribution if this is a new high
                        if (
                            contrib["contributor_aggregate_ytd"]
                            > donorMap["groups"][group]["rollup"][
                                contrib["contributor_name"]
                            ]["contributor_aggregate_ytd"]
                        ):
                            donorMap["groups"][group]["rollup"][
                                contrib["contributor_name"]
                            ]["contributor_aggregate_ytd"] = contrib[
                                "contributor_aggregate_ytd"
                            ]

                # Update the total contributions count and amount for the group, regardless of whether this is going in
                # a rollup
                donorMap["groups"][group]["total"] = round(
                    donorMap["groups"][group]["total"]
                    + contrib["contribution_receipt_amount"],
                    2,
                )
                donorMap["contributions_count"] += 1

            if contribs_count >= data["pagination"]["count"]:
                break
            else:
                last_index = data["pagination"]["last_indexes"]["last_index"]
                last_contribution_receipt_amount = data["pagination"]["last_indexes"][
                    "last_contribution_receipt_amount"
                ]

        for group, data in donorMap["groups"].items():
            # Combine the rollups with the contributions list
            for name in data["rollup"]:
                if data["rollup"][name]["total"] == 1:
                    # If there's only one contribution, don't roll up. Throw away rollup fields.
                    data["rollup"][name] = pick(
                        data["rollup"][name], CONTRIBUTION_FIELDS
                    )
                else:
                    # Throw away fields that only pertain to one contribution, since this will be a rollup
                    data["rollup"][name] = pick(
                        data["rollup"][name],
                        SHARED_CONTRIBUTION_FIELDS + ROLLUP_CONTRIBUTION_FIELDS,
                    )

                # Redact if needed
                if data["rollup"][name]["redacted"]:
                    for k in SHARED_CONTRIBUTION_FIELDS[0:5]:
                        data["rollup"][name][k] = "REDACTED"

                # Add to contribs
                donorMap["groups"][group]["contributions"].append(data["rollup"][name])

            # Sort the per-group contributions list by amount
            donorMap["groups"][group]["contributions"] = sorted(
                donorMap["groups"][group]["contributions"],
                key=(
                    lambda x: x["contribution_receipt_amount"]
                    if "contribution_receipt_amount" in x
                    else x["total_receipt_amount"]
                ),
                reverse=True,
            )
            del donorMap["groups"][group]["rollup"]

        # Turn the map of groups into a list, sorted descending by total contributions
        donor_list = [
            {"company": company, **data} for company, data in donorMap["groups"].items()
        ]
        donorMap["groups"] = sorted(donor_list, key=lambda x: x["total"], reverse=True)

        db.client.collection("contributions").document(committee_id).set(donorMap)
