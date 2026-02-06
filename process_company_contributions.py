from get_missing_recipients import get_missing_recipient_data


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
        for ind in related_individuals:
            ind_data = individuals_data.get(ind["id"])
            if not ind_data:
                continue
            ind_contribs = ind_data.get("contributions", {})
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
                contributions[recipient]["contributions"].extend(
                    contribs_with_attribution
                )
                contributions[recipient]["total"] += group_data["total"]

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
