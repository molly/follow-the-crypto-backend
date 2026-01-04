"""
Shared utility functions for company contribution processing.
"""
from requests_cache import CachedSession
from get_missing_recipients import get_missing_recipient_data


def update_company_contributions_selective(db, company_ids):
    """
    Update company contributions for only specific companies.

    Args:
        db: Database instance with Firestore client
        company_ids: List of company IDs to update

    Returns:
        set: Set of new recipient IDs that were discovered
    """
    recipients_doc = db.client.collection("allRecipients").document("recipients").get()
    all_recipients = recipients_doc.to_dict() if recipients_doc.exists else {}
    if not all_recipients:
        all_recipients = {}
    new_recipients = set()

    # Batch fetch all companies at once
    companies_data = {}
    if company_ids:
        company_refs = [
            db.client.collection("companies").document(company_id)
            for company_id in company_ids
        ]
        for company_doc in db.client.get_all(company_refs):
            if company_doc.exists:
                company_data = company_doc.to_dict()
                if company_data:
                    companies_data[company_doc.id] = company_data

    # Collect all unique individual IDs we need to fetch
    all_individual_ids = set()
    for company_data in companies_data.values():
        related_individuals = company_data.get("relatedIndividuals", [])
        for ind in related_individuals:
            all_individual_ids.add(ind["id"])

    # Batch fetch all individuals at once
    individuals_data = {}
    if all_individual_ids:
        individual_refs = [
            db.client.collection("individuals").document(ind_id)
            for ind_id in all_individual_ids
        ]
        for ind_doc in db.client.get_all(individual_refs):
            if ind_doc.exists:
                individuals_data[ind_doc.id] = ind_doc.to_dict()

    # Process each company
    for company_id in company_ids:
        company = companies_data.get(company_id)
        if not company:
            continue

        contributions = company.get("contributions", {})
        related_individuals = company.get("relatedIndividuals", [])

        # Add contributions from related individuals
        for ind in related_individuals:
            ind_data = individuals_data.get(ind["id"])
            if not ind_data:
                continue

            ind_contribs = ind_data.get("contributions", [])
            for group_data in ind_contribs:
                recipient = group_data["committee_id"]
                contribs_with_attribution = [
                    {**c, "isIndividual": True, "individual": ind["id"]}
                    for c in group_data["contributions"]
                ]
                if recipient not in contributions:
                    contributions[recipient] = {
                        "contributions": [],
                        "total": 0,
                        "committee_id": recipient,
                    }

                # Add to recipients tracking
                if recipient not in all_recipients:
                    new_recipients.add(recipient)
                    all_recipients[recipient] = {
                        "committee_id": recipient,
                        "candidate_details": {},
                        "needs_data": True,
                    }

                contributions[recipient]["contributions"].extend(
                    contribs_with_attribution
                )
                contributions[recipient]["total"] += group_data["total"]

        # Update the company with new contribution data
        sorted_contributions = sorted(
            contributions.values(), key=lambda x: x["total"], reverse=True
        )
        db.client.collection("companies").document(company_id).set(
            {"contributions": sorted_contributions}, merge=True
        )

    # Update recipients if there are new ones
    if new_recipients:
        session = CachedSession("cache", backend="filesystem")
        recipients = get_missing_recipient_data(all_recipients, db, session)
        db.client.collection("allRecipients").document("recipients").set(recipients)

    return new_recipients
