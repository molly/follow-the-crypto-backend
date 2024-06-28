from utils import pick

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


def redact_contribution(d):
    if "redacted" in d and d["redacted"]:
        for k in CONTRIBUTION_FIELDS[0:5]:
            d[k] = "REDACTED"
    return d


def pick_and_redact_contribution(d, keys):
    res = pick(d, keys)
    res = redact_contribution(res)
    return res


def is_redacted(contrib, allowlists):
    """Redact any names for occupations not captured within the occupationAllowlist."""
    if contrib.get("entity_type") in {"ORG", "PAC"} or (
        not contrib["contributor_first_name"] and not contrib["contributor_last_name"]
    ):
        # No redactions needed if this isn't an individual
        return False
    if not contrib["contributor_occupation"]:
        # Redact if the contributor is missing, just in case
        return True
    occupation = contrib["contributor_occupation"].upper()
    return occupation not in allowlists["equals"] and not allowlists["contains"].search(
        occupation
    )


def process_contribution(contrib, db, donorMap):
    redacted = is_redacted(contrib, db.occupation_allowlist)
    if redacted:
        # Mark to redact later
        contrib["redacted"] = True

    # Get group name
    group = contrib["contributor_employer"] or contrib["contributor_name"]
    if group and group in db.individual_employers:
        group = contrib["contributor_name"]
    elif not group:
        group = "UNKNOWN"
    elif group in db.company_aliases:
        group = db.company_aliases[group]

    # Add group to map if the group isn't already in there
    if group not in donorMap["groups"]:
        donorMap["groups"][group] = {
            "contributions": [],
            "rollup": {},
            "total": 0,
        }

    if contrib["line_number"] == "12" or contrib["line_number"].lower() == "11c":
        # This was a transfer from another committee and shouldn't be double counted
        donorMap["total_transferred"] = round(
            donorMap["total_transferred"] + contrib["contribution_receipt_amount"],
            2,
        )
    else:
        donorMap["total_contributed"] = round(
            donorMap["total_contributed"] + contrib["contribution_receipt_amount"],
            2,
        )

    if contrib["contribution_receipt_amount"] >= ROLLUP_THRESHOLD:
        # Record the individual contribution if it's large
        donorMap["groups"][group]["contributions"].append(redact_contribution(contrib))
    else:
        # Add the contribution to a rollup.
        # Note we don't redact here, that happens later
        if contrib["contributor_name"] not in donorMap["groups"][group]["rollup"]:
            # Initialize the rollup group
            donorMap["groups"][group]["rollup"][contrib["contributor_name"]] = {
                **contrib,
                "oldest": contrib["contribution_receipt_date"],
                "newest": contrib["contribution_receipt_date"],
                "total": 1,
                "total_receipt_amount": round(
                    contrib["contribution_receipt_amount"], 2
                ),
            }
        else:
            donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                "total"
            ] += 1
            donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                "total_receipt_amount"
            ] += round(contrib["contribution_receipt_amount"], 2)

            # Set newest/oldest dates
            if (
                contrib["contribution_receipt_date"]
                < donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                    "oldest"
                ]
            ):
                donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                    "oldest"
                ] = contrib["contribution_receipt_date"]
            if (
                contrib["contribution_receipt_date"]
                > donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                    "newest"
                ]
            ):
                donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                    "newest"
                ] = contrib["contribution_receipt_date"]

            # Update the aggregate YTD contribution if this is a new high
            if (
                contrib["contributor_aggregate_ytd"]
                > donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                    "contributor_aggregate_ytd"
                ]
            ):
                donorMap["groups"][group]["rollup"][contrib["contributor_name"]][
                    "contributor_aggregate_ytd"
                ] = contrib["contributor_aggregate_ytd"]

    # Update the total contributions count and amount for the group, regardless of whether this is going in
    # a rollup
    donorMap["groups"][group]["total"] = round(
        donorMap["groups"][group]["total"] + contrib["contribution_receipt_amount"],
        2,
    )
    donorMap["contributions_count"] += 1
    return contrib


def process_committee_contributions(db):
    raw_committee_contributions = db.client.collection("rawContributions").stream()
    for doc in raw_committee_contributions:
        committee_id, contributions = doc.id, doc.to_dict()
        all_contribs = []
        donorMap = {
            "contributions_count": 0,
            "groups": {},
            "recent": [],
            "total_contributed": 0,
            "total_transferred": 0,
        }

        redacted_count = 0
        for contrib in contributions["transactions"]:
            details = process_contribution(contrib, db, donorMap)
            if details is not None:
                all_contribs.append(details)
                if details.get("redacted"):
                    redacted_count += 1

        for group, data in donorMap["groups"].items():
            # Combine the rollups with the contributions list
            for name in data["rollup"]:
                if data["rollup"][name]["total"] == 1:
                    # If there's only one contribution, don't roll up. Throw away rollup fields.
                    data["rollup"][name] = pick_and_redact_contribution(
                        data["rollup"][name], CONTRIBUTION_FIELDS
                    )
                else:
                    # Throw away fields that only pertain to one contribution, since this will be a rollup
                    data["rollup"][name] = pick_and_redact_contribution(
                        data["rollup"][name],
                        SHARED_CONTRIBUTION_FIELDS + ROLLUP_CONTRIBUTION_FIELDS,
                    )

                # Add to contribs
                donorMap["groups"][group]["contributions"].append(data["rollup"][name])

            # Sort the per-group contributions list by amount, then by receipt date
            donorMap["groups"][group]["contributions"] = sorted(
                donorMap["groups"][group]["contributions"],
                key=lambda x: (
                    x["contribution_receipt_amount"]
                    if "contribution_receipt_amount" in x
                    else x["total_receipt_amount"],
                    x["contribution_receipt_date"]
                    if "contribution_receipt_date" in x
                    else None,
                ),
                reverse=True,
            )
            del donorMap["groups"][group]["rollup"]

            # Sort the list of all contributions by receipt date
            donorMap["recent"] = sorted(
                all_contribs,
                key=lambda x: x["contribution_receipt_date"],
                reverse=True,
            )[:10]

        # Turn the map of groups into a list, sorted descending by total contributions
        donor_list = [
            {"company": company, **data} for company, data in donorMap["groups"].items()
        ]
        donorMap["groups"] = sorted(donor_list, key=lambda x: x["total"], reverse=True)

        db.client.collection("contributions").document(committee_id).set(donorMap)
