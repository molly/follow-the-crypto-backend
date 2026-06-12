from google.cloud import firestore

from get_missing_recipients import get_missing_recipient_data
from process_individual_contributions import handle_memo_items
from recipient_utils import (
    get_all_recipients,
    resolve_recipient_party,
    set_all_recipients,
)
from utils import compare_names_lastfirst, get_sector_keys, pick

ROLLUP_THRESHOLD = 10000


def get_contribution_id(contrib, committee_id=""):
    """Generate a stable identifier for a contribution for manual review matching.

    Individual transactions (large contributions and single-contribution rollups)
    are keyed by their FEC transaction_id. Multi-contribution rollups have no single
    transaction_id, so they are keyed by recipient committee and normalized contributor
    name. Both are stable across pipeline runs: the rollup's summed amount and oldest
    date are NOT used, because the incremental fetch grows the rollup over time and any
    drift in those values would silently break the match and wipe the manual review.
    """
    if contrib.get("transaction_id"):
        return f"txn_{contrib['transaction_id']}"
    name = contrib.get("contributor_name", "")
    return f"rollup_{committee_id}_{name}"


def load_all_existing_reviews(db):
    """Load all existing manualReview flags from the companies collection.

    Must be called before any processing to capture reviews before contributions
    are overwritten by the pipeline.
    """
    all_reviews = {}
    for doc in db.client.collection("companies").stream():
        contributions_data = doc.to_dict().get("contributions", [])
        # Handle both list format (from previous pipeline run) and dict format.
        # Pair each group with its recipient committee_id so rollup reviews can be
        # matched on a stable (committee, contributor) key.
        if isinstance(contributions_data, dict):
            groups = [
                (committee_id, group)
                for committee_id, group in contributions_data.items()
            ]
        else:
            groups = [
                (group.get("committee_id", ""), group) for group in contributions_data
            ]
        reviews = {}
        for committee_id, group in groups:
            for contrib in group.get("contributions", []):
                review = contrib.get("manualReview")
                if review and review.get("reviewed"):
                    contrib_id = get_contribution_id(contrib, committee_id)
                    reviews[contrib_id] = {
                        "manualReview": review,
                        "description": contrib.get("description"),
                    }
        if reviews:
            all_reviews[doc.id] = reviews
    return all_reviews

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
    # Load existing manualReview flags BEFORE the pipeline overwrites contributions
    existing_reviews = load_all_existing_reviews(db)

    all_recipients = get_all_recipients(db)
    new_recipients = set()

    # Track individual contribution transaction IDs attributed via raw company FEC data.
    # Individual contributions may be returned by multiple companies' employer/name
    # searches (e.g. a founder listed under two related companies), so we deduplicate
    # globally to prevent the same contribution from being attributed to more than one
    # company.
    raw_attributed_individual_ids = set()

    for doc in db.client.collection("rawCompanyContributions").stream():
        company_id, company = doc.id, doc.to_dict()
        contributions = company["contributions"]

        # Group by (contributor, date) and apply memo item handling to avoid
        # double-counting pass-through contributions (e.g. via JFCs).
        grouped_by_contributor_date = {}
        for contrib in contributions:
            key = (contrib.get("contributor_name", ""), contrib["contribution_receipt_date"])
            if key not in grouped_by_contributor_date:
                grouped_by_contributor_date[key] = []
            grouped_by_contributor_date[key].append(contrib)

        deduped_contributions = []
        for group in grouped_by_contributor_date.values():
            if any(c.get("memo_code") for c in group):
                deduped_contributions.extend(handle_memo_items(group))
            else:
                deduped_contributions.extend(group)

        grouped_by_recipient = {}
        for contrib in deduped_contributions:
            recipient = contrib["committee_id"]
            if recipient not in all_recipients:
                new_recipients.add(recipient)
                all_recipients[recipient] = {
                    "committee_id": recipient,
                    "candidate_details": {},
                    "needs_data": True,
                }

            # Deduplicate individual contributions across companies. The same
            # person's contribution can appear in multiple companies' raw FEC data
            # when their name or employer matches several search terms. Only
            # attribute it to the first company that claims it.
            transaction_id = contrib.get("transaction_id")
            if (
                contrib.get("contributor_first_name")
                and contrib.get("contributor_last_name")
                and transaction_id
            ):
                if transaction_id in raw_attributed_individual_ids:
                    continue
                raw_attributed_individual_ids.add(transaction_id)

            if recipient not in grouped_by_recipient:
                grouped_by_recipient[recipient] = {
                    "contributions": [],
                    "total": 0,
                    "committee_id": recipient,
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
    set_all_recipients(db, recipients)

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

    # Reverse index of reported (dark-money) gifts: donor company -> total amount.
    # `knownDonors` is hand-curated onto each non-disclosing RECIPIENT's constant
    # (e.g. public-first-action), so to surface a gift as the DONOR's spending we
    # invert it. Only entries that point at a tracked company (idType defaults to
    # "company") can be attributed; name-only entries have no donor page to credit.
    reported_by_company = {}
    for recipient_const in db.companies.values():
        for donor in recipient_const.get("knownDonors", []) or []:
            donor_id = donor.get("id")
            if not donor_id or donor.get("idType", "company") != "company":
                continue
            reported_by_company[donor_id] = reported_by_company.get(
                donor_id, 0
            ) + (donor.get("amount") or 0)

    # Summarize spending by party. `total`/`reported` are kept distinct: reported
    # dark-money gifts often pass through tracked recipients that ALSO appear in
    # these sums via their own FEC outbound, so blending the two into one grand
    # figure would double-count the pass-through. Per-company `total` folds them in
    # (a single company can't double-count itself); the roll-ups keep them apart.
    all_companies_total = 0
    all_companies_fec_total = 0
    all_companies_reported = 0
    all_companies_to_tracked = 0
    all_companies_by_party = {}
    all_companies_by_company = {}
    sector_companies_data = {
        "crypto": {
            "total": 0, "fec_total": 0, "reported": 0,
            "to_tracked": 0, "by_party": {}, "by_company": {},
        },
        "ai": {
            "total": 0, "fec_total": 0, "reported": 0,
            "to_tracked": 0, "by_party": {}, "by_company": {},
        },
    }
    # Committees the site actively tracks (keys of constants/committees). A
    # contribution counts as "to a tracked committee" if its recipient is in
    # this set — same definition used for is_tracked in pacs.py.
    tracked_committee_ids = set(db.committees.keys()) if db.committees else set()
    # Track individual contribution transaction IDs that have already been attributed
    # to a company, to prevent double-counting when an individual is associated with
    # multiple companies (e.g. a founder of two related companies).
    # Pre-populate from raw FEC data attributions so that contributions already
    # captured via employer/name searches are not re-attributed via the individuals
    # collection.
    globally_attributed_individual_transaction_ids = set(raw_attributed_individual_ids)
    for company_id, company in companies_list:
        contributions = company.get("contributions", {})
        related_individuals = company.get("relatedIndividuals", [])

        # Collect existing transaction_ids from company contributions to dedup
        existing_transaction_ids = set()
        # Also add individual attribution to company contributions where applicable
        for group_data in contributions.values():
            for c in group_data.get("contributions", []):
                if "transaction_id" in c:
                    existing_transaction_ids.add(c["transaction_id"])
                # Check if this is an individual contribution (has first and last name).
                # "N/A" in name fields means the field was filled with a placeholder,
                # not a real name, so exclude those.
                # These will have already been filtered by occupation allowlist in company_spending.py
                if (
                    c.get("contributor_first_name")
                    and c.get("contributor_first_name").upper() != "N/A"
                    and c.get("contributor_last_name")
                    and c.get("contributor_last_name").upper() != "N/A"
                ):
                    c["isIndividual"] = True
                    contributor_name = c.get("contributor_name", "")
                    for ind in related_individuals:
                        if compare_names_lastfirst(ind["name"], contributor_name):
                            c["individual"] = ind["id"]
                            break

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
                    transaction_id = c.get("transaction_id")
                    if transaction_id in existing_transaction_ids:
                        # This contribution is already in this company's raw data.
                        # Register it globally so other companies that share this
                        # individual won't also claim it via the individuals collection.
                        if transaction_id is not None:
                            globally_attributed_individual_transaction_ids.add(
                                transaction_id
                            )
                        continue
                    if transaction_id in globally_attributed_individual_transaction_ids:
                        continue
                    contribs_with_attribution.append(
                        {**c, "isIndividual": True, "individual": ind["id"]}
                    )
                    deduped_total += c.get("contribution_receipt_amount", 0)
                    existing_transaction_ids.add(transaction_id)
                    globally_attributed_individual_transaction_ids.add(transaction_id)
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
                # Convert to uppercase for case-insensitive matching.
                normalized_name = contributor_name.upper()
                if ", " in contributor_name:
                    parts = contributor_name.split(", ", 1)
                    if len(parts) == 2:
                        last = parts[0].upper()
                        first_parts = parts[1].split()
                        if first_parts:
                            first = first_parts[0].upper()
                            normalized_name = f"{last}, {first}"

                if amount >= ROLLUP_THRESHOLD or normalized_name == "N/A":
                    # Large contributions are kept separate, as are contributions
                    # with no meaningful name — keeping each N/A entry distinct
                    # prevents unrelated entities from being merged under one "N/A" group.
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

        # Merge back manualReview flags and recompute group totals excluding omitted
        company_reviews = existing_reviews.get(company_id, {})
        for committee_id, group_data in contributions.items():
            reviewed_total = 0
            for contrib in group_data["contributions"]:
                contrib_id = get_contribution_id(contrib, committee_id)
                if contrib_id in company_reviews:
                    saved = company_reviews[contrib_id]
                    contrib["manualReview"] = saved["manualReview"]
                    if saved.get("description"):
                        contrib["description"] = saved["description"]
                review = contrib.get("manualReview")
                if not (review and review.get("status") == "omit"):
                    amount = (
                        contrib.get("contribution_receipt_amount")
                        or contrib.get("total_receipt_amount", 0)
                    )
                    reviewed_total += amount
            group_data["total"] = round(reviewed_total, 2)

        recipient_embed_keys = [
            "committee_id",
            "committee_name",
            "link",
            "description",
            "designation_full",
            "party",
            "candidate_ids",
            "sponsor_candidate_ids",
            "candidate_details",
        ]

        party_summary = {}
        to_tracked = 0
        for committee_id, group_data in contributions.items():
            if committee_id in tracked_committee_ids:
                to_tracked += group_data["total"]
            party = "UNK"
            if committee_id in recipients:
                committee = recipients[committee_id]
                party = resolve_recipient_party(committee)
                recipient_data = {k: committee[k] for k in recipient_embed_keys if k in committee}
                group_data["recipient"] = recipient_data
            if party not in party_summary:
                party_summary[party] = 0
            party_summary[party] += group_data["total"]

        # `total` is the company's full political spending: FEC contributions plus
        # publicly-reported dark-money gifts. `fec_total` is the FEC-only portion
        # that `by_party` reconciles to and that destination-specific breakdowns
        # (party, flow, beneficiaries) must use — reported money has no party or
        # tracked recipient. `reported` carries the dark-money figure on its own.
        fec_total = sum(party_summary.values())
        reported_total = reported_by_company.get(company_id, 0)
        company_total = fec_total + reported_total
        company_entry = {
            "total": round(company_total, 2),
            "fec_total": round(fec_total, 2),
            "reported": round(reported_total, 2),
            "to_tracked": round(to_tracked, 2),
            "by_party": {k: round(v, 2) for k, v in party_summary.items()},
        }
        all_companies_by_company[company_id] = company_entry
        all_companies_total += company_total
        all_companies_fec_total += fec_total
        all_companies_reported += reported_total
        all_companies_to_tracked += to_tracked
        for party, amount in party_summary.items():
            if party not in all_companies_by_party:
                all_companies_by_party[party] = 0
            all_companies_by_party[party] += amount

        company_sector = db.companies.get(company_id, {}).get("sector")
        for key in get_sector_keys(company_sector):
            if key == "all":
                continue
            sector_data = sector_companies_data[key]
            sector_data["by_company"][company_id] = company_entry
            sector_data["total"] += company_total
            sector_data["fec_total"] += fec_total
            sector_data["reported"] += reported_total
            sector_data["to_tracked"] += to_tracked
            for party, amount in party_summary.items():
                if party not in sector_data["by_party"]:
                    sector_data["by_party"][party] = 0
                sector_data["by_party"][party] += amount

        sorted_contributions = sorted(
            contributions.values(), key=lambda x: x["total"], reverse=True
        )
        # Stamp updated_at so the pipeline's input-staleness check
        # (StateTracker._collection_modified_since) can detect that this
        # collection changed. Downstream tasks like summarize_recipients declare
        # `inputs=["companies"]` expecting this field; without it, their staleness
        # guard silently never fires and recipientDetails goes stale.
        db.client.collection("companies").document(company_id).set(
            {
                "party_summary": party_summary,
                "contributions": sorted_contributions,
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )

    db.client.collection("totals").document("companies").set(
        {
            "all": {
                "total": round(all_companies_total, 2),
                "fec_total": round(all_companies_fec_total, 2),
                "reported": round(all_companies_reported, 2),
                "to_tracked": round(all_companies_to_tracked, 2),
                "by_party": {k: round(v, 2) for k, v in all_companies_by_party.items()},
                "by_company": all_companies_by_company,
            },
            "crypto": {
                "total": round(sector_companies_data["crypto"]["total"], 2),
                "fec_total": round(sector_companies_data["crypto"]["fec_total"], 2),
                "reported": round(sector_companies_data["crypto"]["reported"], 2),
                "to_tracked": round(sector_companies_data["crypto"]["to_tracked"], 2),
                "by_party": {
                    k: round(v, 2)
                    for k, v in sector_companies_data["crypto"]["by_party"].items()
                },
                "by_company": sector_companies_data["crypto"]["by_company"],
            },
            "ai": {
                "total": round(sector_companies_data["ai"]["total"], 2),
                "fec_total": round(sector_companies_data["ai"]["fec_total"], 2),
                "reported": round(sector_companies_data["ai"]["reported"], 2),
                "to_tracked": round(sector_companies_data["ai"]["to_tracked"], 2),
                "by_party": {
                    k: round(v, 2)
                    for k, v in sector_companies_data["ai"]["by_party"].items()
                },
                "by_company": sector_companies_data["ai"]["by_company"],
            },
        }
    )

    return new_recipients
