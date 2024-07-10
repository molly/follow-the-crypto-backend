from process_individual_contributions import get_missing_recipient_data


def process_company_contributions(db):
    all_recipients = (
        db.client.collection("allRecipients").document("recipients").get().to_dict()
    )
    new_recipients = set()

    for doc in db.client.collection("rawCompanyContributions").stream():
        company_id, company = doc.id, doc.to_dict()
        contributions = company["contributions"]

        grouped_by_recipient = {}
        for contrib in contributions:
            recipient = contrib["committee_id"]
            if recipient not in grouped_by_recipient:
                grouped_by_recipient[recipient] = {"contributions": [], "total": 0}
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
    recipients = get_missing_recipient_data(all_recipients, db)
    db.client.collection("allRecipients").document("recipients").set(recipients)

    # Bring in spending by related individuals
    # Summarize spending by party
    for doc in db.client.collection("companies").stream():
        company_id, company = doc.id, doc.to_dict()
        contributions = company.get("contributions", {})
        related_individuals = company.get("relatedIndividuals", [])
        for ind in related_individuals:
            ind_data = (
                db.client.collection("individuals").document(ind["id"]).get().to_dict()
            )
            ind_contribs = ind_data.get("contributions", {})
            for recipient, group_data in ind_contribs.items():
                contribs_with_attribution = [
                    {**c, "isIndividual": True, "individual": ind["id"]}
                    for c in group_data["contributions"]
                ]
                if recipient not in contributions:
                    contributions[recipient] = {"contributions": [], "total": 0}
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
                        c["party"] for c in committee["candidate_details"].values()
                    ]
                    if (
                        len(set(parties)) == 1
                        and parties[0] is not None
                        and not parties[0].startswith("N")
                    ):
                        party = parties[0]
                if party not in party_summary:
                    party_summary[party] = 0
            party_summary[party] += group_data["total"]

        db.client.collection("companies").document(company_id).set(
            {"party_summary": party_summary, "contributions": contributions}, merge=True
        )

    return new_recipients
