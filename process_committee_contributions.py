"""
Process committee contributions from rawContributions to contributions collection.
"""

import logging
from datetime import datetime
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
    "link",
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


def get_contribution_id(contrib):
    """Generate a unique identifier for a contribution for manual review matching."""
    if "transaction_id" in contrib and contrib["transaction_id"]:
        return f"txn_{contrib['transaction_id']}"

    # For rollups or contributions without transaction_id, create a composite key
    name = contrib.get("contributor_name", "")
    amount = contrib.get("contribution_receipt_amount") or contrib.get(
        "total_receipt_amount", 0
    )
    date = contrib.get("contribution_receipt_date") or contrib.get("oldest", "")
    return f"rollup_{name}_{amount}_{date}"


def load_manually_reviewed_contributions(db, committee_id):
    """Load existing manually reviewed contributions from the processed contributions."""
    try:
        existing = db.client.collection("contributions").document(committee_id).get()
        if not existing.exists:
            return {}

        data = existing.to_dict()
        manually_reviewed = {}

        # Check all groups for manually reviewed contributions
        for group in data.get("groups", []):
            for contrib in group.get("contributions", []):
                if "manualReview" in contrib and contrib["manualReview"].get(
                    "reviewed"
                ):
                    contrib_id = get_contribution_id(contrib)
                    manually_reviewed[contrib_id] = contrib

        # Also check by_date list
        for contrib in data.get("by_date", []):
            if "manualReview" in contrib and contrib["manualReview"].get("reviewed"):
                contrib_id = get_contribution_id(contrib)
                manually_reviewed[contrib_id] = contrib

        return manually_reviewed
    except Exception as e:
        logging.warning(f"Error loading manually reviewed contributions: {e}")
        return {}


def should_skip_contribution(contrib):
    """Check if a contribution should be skipped due to manual review status."""
    if "manualReview" not in contrib:
        return False

    manual_review = contrib["manualReview"]
    if not manual_review.get("reviewed"):
        return False

    # Skip contributions marked as omit
    return manual_review.get("status") == "omit"


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
    if contrib.get("claimed", False):
        return False
    if contrib.get("entity_type") in {"ORG", "PAC", "COM"} or (
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


def get_claimed_contributions(individuals, committee_id):
    claimed_contributions = []
    for individual in individuals.values():
        if (
            "claimedContributions" in individual
            and len(individual["claimedContributions"]) > 0
        ):
            for contrib in individual["claimedContributions"]:
                if contrib["committee_id"] == committee_id:
                    claimed_contributions.append({**contrib, "claimed": True})
    return claimed_contributions


def process_contribution(contrib, db, donorMap):
    redacted = is_redacted(contrib, db.occupation_allowlist)
    if redacted:
        # Mark to redact later
        contrib["redacted"] = True

    # Get group name
    group = None
    if (
        "contributor_employer" in contrib
        and contrib["contributor_employer"]
        and contrib["contributor_employer"] != "N/A"
    ):
        group = contrib["contributor_employer"]
    else:
        group = contrib["contributor_name"]
    if group and group in db.individual_employers:
        group = contrib["contributor_name"]
    elif not group:
        group = "UNKNOWN"
    elif group in db.company_aliases:
        group = db.company_aliases[group]

    link = None
    for company in db.companies.values():
        if company["name"].upper() == group or any(
            alias.upper() == group for alias in company.get("aliases", [])
        ):
            link = "/companies/" + company["id"]
            break
    if not link:
        for committee in db.committees.values():
            if committee["name"].upper() == group:
                link = "/committees/" + committee["id"]
                break
    if not link:
        for individual in db.individuals.values():
            if individual["name"].upper() == group:
                link = "/individuals/" + individual["id"]
                break

    if link:
        contrib["link"] = link

    # Add group to map if the group isn't already in there
    if group not in donorMap["groups"]:
        donorMap["groups"][group] = {
            "contributions": [],
            "rollup": {},
            "total": 0,
        }
        if link:
            donorMap["groups"][group]["link"] = link
    elif contrib.get("claimed", False):
        # Warn if there are claimed contributions that may duplicate ones coming from the FEC
        if any(
            ["claimed" not in x for x in donorMap["groups"][group]["contributions"]]
        ):
            logging.warning(
                "Claimed contribution committee also appears in FEC data, check for duplicates.",
                {"claimed_contrib": contrib},
            )
            print(
                "Claimed contribution committee also appears in FEC data, check for duplicates. Ind: {} Committee: {}".format(
                    contrib.get("contributor_name"), contrib.get("committee_id")
                )
            )

    if (
        contrib.get("line_number") == "12"
        or contrib.get("line_number", "").lower() == "11c"
    ):
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
            if "contributor_aggregate_ytd" in contrib:
                current_aggregate = contrib["contributor_aggregate_ytd"] or 0
                rollup_entry = donorMap["groups"][group]["rollup"][
                    contrib["contributor_name"]
                ]
                existing_aggregate = rollup_entry.get("contributor_aggregate_ytd") or 0

                if current_aggregate > existing_aggregate:
                    rollup_entry["contributor_aggregate_ytd"] = contrib[
                        "contributor_aggregate_ytd"
                    ]

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
    individuals = (
        db.client.collection("constants").document("individuals").get().to_dict()
    )
    for doc in raw_committee_contributions:
        committee_id, contributions = doc.id, doc.to_dict()

        # Load existing manually reviewed contributions
        manually_reviewed = load_manually_reviewed_contributions(db, committee_id)
        manually_reviewed_ids = set(manually_reviewed.keys())

        all_contribs = []
        donorMap = {
            "contributions_count": 0,
            "groups": {},
            "by_date": [],
            "total_contributed": 0,
            "total_transferred": 0,
        }

        redacted_count = 0
        for contrib in contributions["transactions"]:
            # Skip if this contribution has been manually reviewed
            contrib_id = get_contribution_id(contrib)
            if contrib_id in manually_reviewed_ids:
                continue

            # Skip if marked as omit
            if should_skip_contribution(contrib):
                continue

            details = process_contribution(contrib, db, donorMap)
            if details is not None:
                all_contribs.append(details)
                if details.get("redacted"):
                    redacted_count += 1

        # Get any claimed contributions
        claimed = get_claimed_contributions(individuals, committee_id)
        for contrib in claimed:
            # Skip if this contribution has been manually reviewed
            contrib_id = get_contribution_id(contrib)
            if contrib_id in manually_reviewed_ids:
                continue

            # Skip if marked as omit
            if should_skip_contribution(contrib):
                continue

            all_contribs.append(contrib)
            process_contribution(contrib, db, donorMap)

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

            # Delete the rollup dict, we've merged it into contributions
            del donorMap["groups"][group]["rollup"]

        # Merge manually reviewed contributions back in
        for contrib_id, contrib in manually_reviewed.items():
            status = contrib.get("manualReview", {}).get("status")

            # For "omit" contributions, store minimal data to preserve the manual review decision
            # This prevents them from being reprocessed on subsequent runs
            if status == "omit":
                # Create minimal contribution with just ID fields and manualReview
                minimal_contrib = {
                    "manualReview": contrib["manualReview"],
                }

                # Include description if present (top-level field)
                if "description" in contrib:
                    minimal_contrib["description"] = contrib["description"]

                # Include ID fields needed for matching
                if "transaction_id" in contrib:
                    minimal_contrib["transaction_id"] = contrib["transaction_id"]
                if "contributor_name" in contrib:
                    minimal_contrib["contributor_name"] = contrib["contributor_name"]
                if "contribution_receipt_amount" in contrib:
                    minimal_contrib["contribution_receipt_amount"] = contrib[
                        "contribution_receipt_amount"
                    ]
                elif "total_receipt_amount" in contrib:
                    minimal_contrib["total_receipt_amount"] = contrib[
                        "total_receipt_amount"
                    ]
                if "contribution_receipt_date" in contrib:
                    minimal_contrib["contribution_receipt_date"] = contrib[
                        "contribution_receipt_date"
                    ]
                elif "oldest" in contrib:
                    minimal_contrib["oldest"] = contrib["oldest"]

                # Add to a special OMITTED group that frontend will filter out
                if "OMITTED" not in donorMap["groups"]:
                    donorMap["groups"]["OMITTED"] = {
                        "contributions": [],
                        "rollup": {},
                        "total": 0,
                    }
                donorMap["groups"]["OMITTED"]["contributions"].append(minimal_contrib)
                continue

            # Only merge back full contributions with status "verified"
            if status != "verified":
                continue

            # Get group name (same logic as in process_contribution)
            group = contrib.get("contributor_employer") or contrib.get(
                "contributor_name", "UNKNOWN"
            )
            if group in db.individual_employers:
                group = contrib.get("contributor_name", "UNKNOWN")
            elif group in db.company_aliases:
                group = db.company_aliases[group]

            # Add group if it doesn't exist
            if group not in donorMap["groups"]:
                donorMap["groups"][group] = {
                    "contributions": [],
                    "rollup": {},
                    "total": 0,
                }
                if "link" in contrib:
                    donorMap["groups"][group]["link"] = contrib["link"]

            # Add to contributions list
            donorMap["groups"][group]["contributions"].append(contrib)

            # Update group total
            amount = contrib.get("contribution_receipt_amount") or contrib.get(
                "total_receipt_amount", 0
            )
            donorMap["groups"][group]["total"] = round(
                donorMap["groups"][group]["total"] + amount, 2
            )

            # Add to all_contribs for by_date list
            all_contribs.append(contrib)

            # Update overall totals
            donorMap["contributions_count"] += 1
            if (
                contrib.get("line_number") == "12"
                or contrib.get("line_number", "").lower() == "11c"
            ):
                donorMap["total_transferred"] = round(
                    donorMap["total_transferred"] + amount, 2
                )
            else:
                donorMap["total_contributed"] = round(
                    donorMap["total_contributed"] + amount, 2
                )

        # Re-sort contributions within each group after adding manually reviewed ones
        for group in donorMap["groups"].values():
            group["contributions"] = sorted(
                group["contributions"],
                # key=lambda x: (
                #     x.get("contribution_receipt_amount")
                #     if "contribution_receipt_amount" in x
                #     else x.get("total_receipt_amount", 0),
                #     x.get("contribution_receipt_date")
                #     if "contribution_receipt_date" in x
                #     else "0",
                # ),
                key=lambda x: x.get("contribution_receipt_date", "0"),
                reverse=True,
            )

        # Re-sort by_date after adding manually reviewed contributions
        donorMap["by_date"] = sorted(
            all_contribs,
            key=lambda x: x.get("contribution_receipt_date", "0"),
            reverse=True,
        )

        # Turn the map of groups into a list, sorted descending by total contributions
        donor_list = [
            {"company": company, **data} for company, data in donorMap["groups"].items()
        ]
        donorMap["groups"] = sorted(donor_list, key=lambda x: x["total"], reverse=True)

        db.client.collection("contributions").document(committee_id).set(donorMap)
