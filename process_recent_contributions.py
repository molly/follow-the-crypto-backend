def process_recent_contributions(db):
    """Generate a recent contributions snapshot across all tracked individuals and companies.

    Reads contributions_by_date from each individual document and flattens
    direct (non-individual-attributed) contributions from each company document,
    then writes the top 50 most recent to contributions/recent.
    """
    recipients_doc = (
        db.client.collection("allRecipients").document("recipients").get()
    )
    all_recipients = recipients_doc.to_dict() if recipients_doc.exists else {}
    all_committees = db.all_committees or {}

    all_contributions = []

    # Build a map of individual_id -> [company_id, ...] from the companies constant
    individual_to_company_ids = {}
    for company_id, company in (db.companies or {}).items():
        for ind in company.get("relatedIndividuals", []):
            ind_id = ind.get("id")
            if ind_id:
                if ind_id not in individual_to_company_ids:
                    individual_to_company_ids[ind_id] = []
                individual_to_company_ids[ind_id].append(company_id)

    non_candidate_committees = db.non_candidate_committees or set()

    def get_committee_info(committee_id):
        committee_name = None
        committee_description = None
        candidate_ids = None
        candidate_details = None
        if committee_id and committee_id in all_recipients:
            recipient = all_recipients[committee_id]
            committee_name = recipient.get("committee_name")
            if committee_id not in non_candidate_committees:
                candidate_ids = (
                    recipient.get("candidate_ids")
                    or recipient.get("sponsor_candidate_ids")
                )
                candidate_details = recipient.get("candidate_details")
        if committee_id and committee_id in all_committees:
            committee_description = all_committees[committee_id]
        return committee_name, committee_description, candidate_ids, candidate_details

    # Collect contributions from tracked individuals
    for doc in db.client.collection("individuals").stream():
        ind_id = doc.id
        ind_data = doc.to_dict()
        ind_constant = db.individuals.get(ind_id, {})
        source_name = ind_constant.get("name", ind_id)

        contributions_by_date = ind_data.get("contributions_by_date", [])
        for contrib in contributions_by_date:
            manual_review = contrib.get("manualReview")
            if manual_review and manual_review.get("status") == "omit":
                continue
            committee_id = contrib.get("committee_id")
            committee_name, committee_description, candidate_ids, candidate_details = get_committee_info(committee_id)
            all_contributions.append(
                {
                    **contrib,
                    "source_id": ind_id,
                    "source_name": source_name,
                    "source_type": "individual",
                    "source_company": ind_constant.get("company", []),
                    "source_company_ids": individual_to_company_ids.get(ind_id, []),
                    "committee_name": committee_name,
                    "committee_description": committee_description,
                    "candidate_ids": candidate_ids,
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
            committee_name, committee_description, candidate_ids, candidate_details = get_committee_info(committee_id)
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
