def process_recent_contributions(db):
    """Generate a recent contributions snapshot across all tracked individuals and companies.

    Reads contributions_by_date from each individual document and flattens
    direct (non-individual-attributed) contributions from each company document,
    then writes the top 50 most recent to contributions/recent.
    """
    all_committees = db.all_committees or {}

    all_contributions = []

    # Build a map of company name -> company_id from the companies constant
    company_name_to_id = {
        company.get("name"): company_id
        for company_id, company in (db.companies or {}).items()
        if company.get("name")
    }

    non_candidate_committees = db.non_candidate_committees or set()

    def get_committee_info(recipient):
        """Extract display info from an embedded recipient dict."""
        if not recipient:
            return None, None, None, None, None
        committee_name = recipient.get("committee_name")
        committee_description = recipient.get("description")
        candidate_details = recipient.get("candidate_details")
        committee_id = recipient.get("committee_id")

        if committee_id and committee_id in all_committees:
            committee_description = all_committees[committee_id]

        candidate_ids = None
        sponsor_candidate_ids = None
        if committee_id and committee_id not in non_candidate_committees:
            candidate_ids = recipient.get("candidate_ids")
        if not candidate_ids:
            sponsor_ids = recipient.get("sponsor_candidate_ids")
            if sponsor_ids:
                sponsor_candidate_ids = sponsor_ids

        return committee_name, committee_description, candidate_ids, sponsor_candidate_ids, candidate_details

    # Collect contributions from tracked individuals
    for doc in db.client.collection("individuals").stream():
        ind_id = doc.id
        ind_data = doc.to_dict()
        ind_constant = db.individuals.get(ind_id, {})
        source_name = ind_constant.get("name", ind_id)

        # Build committee_id -> recipient lookup from the enriched contributions groups
        recipient_by_committee = {}
        for group in ind_data.get("contributions", []):
            c_id = group.get("committee_id")
            if c_id and "recipient" in group:
                recipient_by_committee[c_id] = group["recipient"]

        contributions_by_date = ind_data.get("contributions_by_date", [])
        for contrib in contributions_by_date:
            manual_review = contrib.get("manualReview")
            if manual_review and manual_review.get("status") == "omit":
                continue
            committee_id = contrib.get("committee_id")
            recipient = recipient_by_committee.get(committee_id)
            committee_name, committee_description, candidate_ids, sponsor_candidate_ids, candidate_details = get_committee_info(recipient)
            all_contributions.append(
                {
                    **contrib,
                    "source_id": ind_id,
                    "source_name": source_name,
                    "source_type": "individual",
                    "source_company": ind_constant.get("company", []),
                    "source_company_ids": [
                        company_name_to_id.get(name)
                        for name in ind_constant.get("company", [])
                    ],
                    "committee_name": committee_name,
                    "committee_description": committee_description,
                    "candidate_ids": candidate_ids,
                    "sponsor_candidate_ids": sponsor_candidate_ids,
                    "candidate_details": candidate_details,
                }
            )

    # Collect direct (non-individual-attributed) contributions from tracked companies
    for doc in db.client.collection("companies").stream():
        company_id = doc.id
        company_data = doc.to_dict()
        company_constant = db.companies.get(company_id, {})
        source_name = company_constant.get("name", company_id)

        contributions_groups = company_data.get("contributions", [])
        for group in contributions_groups:
            committee_id = group.get("committee_id")
            recipient = group.get("recipient")
            committee_name, committee_description, candidate_ids, sponsor_candidate_ids, candidate_details = get_committee_info(recipient)
            for contrib in group.get("contributions", []):
                if contrib.get("isIndividual"):
                    continue
                manual_review = contrib.get("manualReview")
                if manual_review and manual_review.get("status") == "omit":
                    continue
                all_contributions.append(
                    {
                        **contrib,
                        "committee_id": committee_id,
                        "source_id": company_id,
                        "source_name": source_name,
                        "source_type": "company",
                        "committee_name": committee_name,
                        "committee_description": committee_description,
                        "candidate_ids": candidate_ids,
                        "sponsor_candidate_ids": sponsor_candidate_ids,
                        "candidate_details": candidate_details,
                    }
                )

    def get_sort_date(contrib):
        return contrib.get("contribution_receipt_date") or contrib.get("newest") or ""

    all_contributions.sort(key=get_sort_date, reverse=True)
    most_recent = all_contributions[:50]

    db.client.collection("contributions").document("recent").set(
        {"all": most_recent}
    )
