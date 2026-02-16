from get_missing_recipients import get_missing_recipient_data
from utils import pick

ROLLUP_THRESHOLD = 10000

SHARED_CONTRIBUTION_FIELDS = [
    "contributor_first_name",
    "contributor_last_name",
    "contributor_name",
    "contributor_occupation",
    "contributor_employer",
    "entity_type",
    "isIndividual",
    "individual",
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


def redact_contribution(d, keys):
    """Pick specified fields from contribution."""
    return pick(d, keys)


def process_company_contributions(db, session):
    recipients_doc = db.client.collection("allRecipients").document("recipients").get()
    all_recipients = recipients_doc.to_dict() if recipients_doc.exists else {}
    if not all_recipients:
        all_recipients = {}
    new_recipients = set()

    for doc in db.client.collection("rawCompanyContributions").stream():
        company_id, company = doc.id, doc.to_dict()
        contributions = company["contributions"]

        grouped_by_recipient = {}
        for contrib in contributions:
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

        db.client.collection("companies").document(company_id).set(
            {"contributions": grouped_by_recipient}, merge=True
        )

    # Get recipient data and record any new committees
    recipients = get_missing_recipient_data(all_recipients, db, session)
    db.client.collection("allRecipients").document("recipients").set(recipients)

    # Bring in spending by related individuals
    # First, collect all unique individual IDs we need to fetch
    all_individual_ids = set()
    companies_list = []
    for doc in db.client.collection("companies").stream():
        company_id, company = doc.id, doc.to_dict()
        companies_list.append((company_id, company))
        related_individuals = company.get("relatedIndividuals", [])
        for ind in related_individuals:
            all_individual_ids.add(ind["id"])

    # Batch fetch all individuals at once
    individuals_data = {}
    if all_individual_ids:
        individual_refs = [
            db.client.collection("individuals").document(ind_id)
            for ind_id in all_individual_ids
        ]
        # Firestore get_all() fetches up to 500 documents at once
        for ind_doc in db.client.get_all(individual_refs):
            if ind_doc.exists:
                individuals_data[ind_doc.id] = ind_doc.to_dict()

    # Summarize spending by party
    for company_id, company in companies_list:
        contributions = company.get("contributions", {})
        related_individuals = company.get("relatedIndividuals", [])

        # Build a mapping from individual names to their IDs for attribution
        individual_name_map = {}
        for ind in related_individuals:
            # Store both the display name and the ID-based name for matching
            individual_name_map[ind["name"].upper()] = ind["id"]
            # Also add the ID-based name (with hyphens replaced by spaces)
            id_name = ind["id"].replace("-", " ").upper()
            individual_name_map[id_name] = ind["id"]
            # Also add "Last, First" format (how FEC data comes in)
            name_parts = ind["name"].split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = name_parts[-1]
                # Convert "First Last" to "Last, First"
                last_first = f"{last_name}, {' '.join(name_parts[:-1])}".upper()
                individual_name_map[last_first] = ind["id"]
                # Also add just "Last, First" without middle names (for matching variations)
                # This handles cases like "CHOI, EMILIE A" matching "Emilie Choi"
                last_first_simple = f"{last_name}, {first_name}".upper()
                individual_name_map[last_first_simple] = ind["id"]

        # Collect existing transaction_ids from company contributions to dedup
        existing_transaction_ids = set()
        # Also add individual attribution to company contributions where applicable
        for group_data in contributions.values():
            for c in group_data.get("contributions", []):
                if "transaction_id" in c:
                    existing_transaction_ids.add(c["transaction_id"])
                # Check if this is an individual contribution (has first and last name)
                # These will have already been filtered by occupation allowlist in company_spending.py
                if c.get("contributor_first_name") and c.get("contributor_last_name"):
                    c["isIndividual"] = True
                    # Check if they match a related individual
                    contributor_name = c.get("contributor_name", "").upper()
                    if contributor_name in individual_name_map:
                        c["individual"] = individual_name_map[contributor_name]
                    else:
                        # Try matching with just first and last name (strip middle initials)
                        # FEC data: "CHOI, EMILIE A" -> "CHOI, EMILIE"
                        parts = contributor_name.split(", ", 1)
                        if len(parts) == 2:
                            last = parts[0]
                            first_parts = parts[1].split()
                            if first_parts:
                                first = first_parts[0]
                                simplified = f"{last}, {first}"
                                if simplified in individual_name_map:
                                    c["individual"] = individual_name_map[simplified]

        for ind in related_individuals:
            ind_data = individuals_data.get(ind["id"])
            if not ind_data:
                continue
            ind_contribs = ind_data.get("contributions", {})
            for group_data in ind_contribs:
                recipient = group_data["committee_id"]
                contribs_with_attribution = []
                deduped_total = 0
                for c in group_data["contributions"]:
                    if c.get("transaction_id") in existing_transaction_ids:
                        continue
                    contribs_with_attribution.append(
                        {**c, "isIndividual": True, "individual": ind["id"]}
                    )
                    deduped_total += c.get("contribution_receipt_amount", 0)
                    existing_transaction_ids.add(c.get("transaction_id"))
                if not contribs_with_attribution:
                    continue
                if recipient not in contributions:
                    contributions[recipient] = {
                        "contributions": [],
                        "total": 0,
                        "committee_id": recipient,
                    }
                contributions[recipient]["contributions"].extend(
                    contribs_with_attribution
                )
                contributions[recipient]["total"] += deduped_total

        # Group and rollup contributions within each committee
        for group_data in contributions.values():
            # Group contributions by contributor
            contributor_rollups = {}
            large_contributions = []

            for contrib in group_data["contributions"]:
                amount = contrib.get("contribution_receipt_amount", 0)
                contributor_name = contrib.get("contributor_name", "UNKNOWN")

                # Normalize name for grouping (strip middle initials and normalize case)
                # "LAST, FIRST MIDDLE" -> "LAST, FIRST" for consistent grouping
                # Convert to uppercase for case-insensitive matching
                normalized_name = contributor_name.upper()
                if ", " in contributor_name:
                    parts = contributor_name.split(", ", 1)
                    if len(parts) == 2:
                        last = parts[0].upper()
                        first_parts = parts[1].split()
                        if first_parts:
                            first = first_parts[0].upper()
                            normalized_name = f"{last}, {first}"

                if amount >= ROLLUP_THRESHOLD:
                    # Large contributions are kept separate
                    large_contributions.append(contrib)
                else:
                    # Small contributions are rolled up by contributor (using normalized name)
                    if normalized_name not in contributor_rollups:
                        contributor_rollups[normalized_name] = {
                            **contrib,
                            "contributor_name": normalized_name,  # Use normalized name
                            "oldest": contrib.get("contribution_receipt_date", ""),
                            "newest": contrib.get("contribution_receipt_date", ""),
                            "total": 1,
                            "total_receipt_amount": round(amount, 2),
                        }
                    else:
                        rollup = contributor_rollups[normalized_name]
                        rollup["total"] += 1
                        rollup["total_receipt_amount"] = round(
                            rollup["total_receipt_amount"] + amount, 2
                        )

                        # Update oldest/newest dates
                        contrib_date = contrib.get("contribution_receipt_date", "")
                        if contrib_date < rollup["oldest"]:
                            rollup["oldest"] = contrib_date
                        if contrib_date > rollup["newest"]:
                            rollup["newest"] = contrib_date

            # Convert rollups to contribution entries
            rollup_contributions = []
            for contributor_name, rollup in contributor_rollups.items():
                if rollup["total"] == 1:
                    # Only one contribution, treat as regular contribution
                    rollup_contributions.append(
                        redact_contribution(rollup, CONTRIBUTION_FIELDS)
                    )
                else:
                    # Multiple contributions, create rollup entry
                    rollup_contributions.append(
                        redact_contribution(
                            rollup, SHARED_CONTRIBUTION_FIELDS + ROLLUP_CONTRIBUTION_FIELDS
                        )
                    )

            # Combine large contributions and rollups, sorted by amount (descending)
            all_contributions = large_contributions + rollup_contributions
            group_data["contributions"] = sorted(
                all_contributions,
                key=lambda x: x.get("contribution_receipt_amount") or x.get("total_receipt_amount", 0),
                reverse=True,
            )

        party_summary = {}
        for committee_id, group_data in contributions.items():
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

        sorted_contributions = sorted(
            contributions.values(), key=lambda x: x["total"], reverse=True
        )
        db.client.collection("companies").document(company_id).set(
            {"party_summary": party_summary, "contributions": sorted_contributions},
            merge=True,
        )

    return new_recipients
