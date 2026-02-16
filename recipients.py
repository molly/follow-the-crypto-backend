from utils import pick, get_beneficiaries

INDIVIDUAL_KEYS = [
    "individual",
    "contributor_name",
    "isIndividual",
]


def group_contributions(new_contributions, existing_contributions, committees):
    for contrib in new_contributions:
        field_keys = (
            INDIVIDUAL_KEYS if contrib.get("isIndividual") else ["contributor_name"]
        )
        group_key = (
            contrib.get("individual", contrib["contributor_name"])
            if contrib.get("isIndividual")
            else contrib["contributor_name"]
        )
        is_rollup = "total_receipt_amount" in contrib
        amount = (
            contrib["total_receipt_amount"]
            if is_rollup
            else contrib["contribution_receipt_amount"]
        )
        oldest_date = (
            contrib["oldest"]
            if is_rollup
            else contrib["contribution_receipt_date"]
        )
        newest_date = (
            contrib["newest"]
            if is_rollup
            else contrib["contribution_receipt_date"]
        )
        if group_key not in existing_contributions:
            existing_contributions[group_key] = {
                **pick(contrib, field_keys),
                "total": 0,
                "committees": [],
                "oldest": oldest_date,
                "newest": newest_date,
            }
        existing_contributions[group_key]["total"] += amount
        if oldest_date < existing_contributions[group_key]["oldest"]:
            existing_contributions[group_key]["oldest"] = oldest_date
        if newest_date > existing_contributions[group_key]["newest"]:
            existing_contributions[group_key]["newest"] = newest_date
        if contrib.get("committee_name"):
            committee_name = contrib.get("committee_name")
            if committee_name not in existing_contributions[group_key]["committees"]:
                existing_contributions[group_key]["committees"].append(committee_name)
        else:
            committee_id = contrib.get("committee_id")
            committee_name = (
                committees.get(committee_id, {}).get("name")
                if committee_id
                else None
            )
            if (
                committee_name
                and committee_name
                not in existing_contributions[group_key]["committees"]
            ):
                existing_contributions[group_key]["committees"].append(committee_name)
    return existing_contributions


def summarize_recipients(db):
    recipients = {}
    all_recipient_committees = (
        db.client.collection("allRecipients").document("recipients").get().to_dict()
    )
    if not all_recipient_committees:
        all_recipient_committees = {}
    for doc in db.client.collection("companies").stream():
        company = doc.to_dict()
        contributions_groups = company["contributions"]
        for group in contributions_groups:
            committee_id = group["committee_id"]
            recipient_committee = all_recipient_committees.get(committee_id)
            beneficiaries = get_beneficiaries(
                group, recipient_committee, db.non_candidate_committees
            )
            for beneficiary in beneficiaries:
                # Normalize candidate IDs (e.g., House ID -> Senate ID for candidates
                # who switched races)
                if beneficiary in db.candidate_aliases:
                    beneficiary = db.candidate_aliases[beneficiary]
                if beneficiary not in recipients:
                    recipients[beneficiary] = {
                        "total": 0,
                        "type": "committee",
                        "contributions": {},
                    }
                    if recipient_committee:
                        recipients[beneficiary]["committee_details"] = pick(
                            recipient_committee,
                            [
                                "committee_type_full",
                                "description",
                                "designation_full",
                                "committee_name",
                                "committee_id",
                            ],
                        )
                    if beneficiary[0] != "C":
                        recipients[beneficiary]["type"] = "candidate"
                        if recipient_committee:
                            candidate_details = recipient_committee.get(
                                "candidate_details", {}
                            ).get(beneficiary, None)
                            if candidate_details:
                                recipients[beneficiary][
                                    "candidate_details"
                                ] = candidate_details
                recipients[beneficiary]["total"] += group["total"]
                recipients[beneficiary]["contributions"] = group_contributions(
                    group["contributions"],
                    recipients[beneficiary]["contributions"],
                    db.committees,
                )

    for recipient, data in recipients.items():
        recipients[recipient]["contributions"] = sorted(
            data["contributions"].values(), key=lambda x: x["total"], reverse=True
        )
        for contrib in recipients[recipient]["contributions"]:
            contrib["committees"] = sorted(
                contrib["committees"], key=lambda x: x.lower()
            )

    order = sorted(
        recipients.keys(), key=lambda x: recipients[x]["total"], reverse=True
    )
    expendituresStream = db.client.collection("candidates").stream()
    expenditures = [doc.to_dict() for doc in expendituresStream]
    candidates_with_expenditures_ids = set(
        [cand.get("candidate_id") for cand in expenditures]
    )
    candidates_without_expenditures = [
        x
        for x in order
        if (
            x[0] != "C"
            and x not in candidates_with_expenditures_ids
            and recipients[x]
            .get("candidate_details", {})
            .get("isRunningThisCycle", False)
        )
    ]

    db.client.collection("allRecipients").document("recipientsWithContribs").set(
        recipients
    )
    db.client.collection("allRecipients").document("recipientsOrder").set(
        {
            "order": order,
            "candidatesWithoutExpendituresOrder": candidates_without_expenditures,
        }
    )
