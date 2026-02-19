from utils import pick, get_beneficiaries

INDIVIDUAL_KEYS = [
    "individual",
    "contributor_name",
    "contributor_occupation",
    "isIndividual",
]


def group_contributions(
    new_contributions,
    existing_contributions,
    committees,
    all_recipient_committees,
    committee_name_to_type,
):
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
            existing_names = [
                c["name"]
                for c in existing_contributions[group_key]["committees"]
            ]
            if committee_name not in existing_names:
                committee_type_full = committee_name_to_type.get(committee_name)
                existing_contributions[group_key]["committees"].append(
                    {"name": committee_name, "committee_type_full": committee_type_full}
                )
        else:
            committee_id = contrib.get("committee_id")
            committee_name = (
                committees.get(committee_id, {}).get("name")
                if committee_id
                else None
            )
            if committee_name:
                existing_names = [
                    c["name"]
                    for c in existing_contributions[group_key]["committees"]
                ]
                if committee_name not in existing_names:
                    committee_type_full = (
                        all_recipient_committees.get(committee_id, {}).get(
                            "committee_type_full"
                        )
                        if committee_id
                        else None
                    )
                    existing_contributions[group_key]["committees"].append(
                        {
                            "name": committee_name,
                            "committee_type_full": committee_type_full,
                        }
                    )
    return existing_contributions


def summarize_recipients(db):
    recipients = {}
    all_recipient_committees = (
        db.client.collection("allRecipients").document("recipients").get().to_dict()
    )
    if not all_recipient_committees:
        all_recipient_committees = {}
    committee_name_to_type = {
        v["committee_name"]: v.get("committee_type_full")
        for v in all_recipient_committees.values()
        if v.get("committee_name")
    }
    for doc in db.client.collection("companies").stream():
        company_id = doc.id
        company = doc.to_dict()
        company_name = db.companies.get(company_id, {}).get("name", company_id)
        contributions_groups = company["contributions"]
        for group in contributions_groups:
            committee_id = group["committee_id"]
            recipient_committee = all_recipient_committees.get(committee_id)
            beneficiaries = get_beneficiaries(
                group, recipient_committee, db.non_candidate_committees
            )
            # Normalize candidate aliases and deduplicate to avoid
            # double-counting when a committee lists multiple IDs for the
            # same candidate (e.g. House ID + Senate ID after switching races)
            seen = set()
            normalized_beneficiaries = []
            for b in beneficiaries:
                if b in db.candidate_aliases:
                    b = db.candidate_aliases[b]
                if b not in seen:
                    seen.add(b)
                    normalized_beneficiaries.append(b)
            for beneficiary in normalized_beneficiaries:
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
                if "by_committee" not in recipients[beneficiary]:
                    recipients[beneficiary]["by_committee"] = {}
                if committee_id not in recipients[beneficiary]["by_committee"]:
                    recipients[beneficiary]["by_committee"][committee_id] = 0
                recipients[beneficiary]["by_committee"][committee_id] += group["total"]
                if company_id not in recipients[beneficiary]["contributions"]:
                    recipients[beneficiary]["contributions"][company_id] = {
                        "company_id": company_id,
                        "company_name": company_name,
                        "total": 0,
                        "contributions": {},
                    }
                recipients[beneficiary]["contributions"][company_id][
                    "total"
                ] += group["total"]
                recipients[beneficiary]["contributions"][company_id][
                    "contributions"
                ] = group_contributions(
                    group["contributions"],
                    recipients[beneficiary]["contributions"][company_id][
                        "contributions"
                    ],
                    db.committees,
                    all_recipient_committees,
                    committee_name_to_type,
                )

    for recipient, data in recipients.items():
        for company_data in data["contributions"].values():
            company_data["contributions"] = sorted(
                company_data["contributions"].values(),
                key=lambda x: x["total"],
                reverse=True,
            )
            for contrib in company_data["contributions"]:
                contrib["committees"] = sorted(
                    contrib["committees"], key=lambda x: x["name"].lower()
                )
        recipients[recipient]["contributions"] = sorted(
            data["contributions"].values(),
            key=lambda x: x["total"],
            reverse=True,
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
