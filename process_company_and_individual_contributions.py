import re


def get_description(contrib):
    desc = contrib.get("memo_text", "")
    if not desc:
        desc = contrib.get("receipt_type_full", "")
    return desc


def dedupe_by_memo(group):
    contribs_by_id = {}
    for contrib in group:
        contribs_by_id[contrib["transaction_id"]] = contrib

    for contrib in group:
        if contrib["transaction_id"] not in contribs_by_id:
            # Possible this was removed in an earlier pass
            continue
        description = get_description(contrib)
        if description:
            c_id_match = re.search(r"\((C\d+)\)", get_description(contrib))
            if c_id_match:
                c_id = c_id_match.group(1)
                contribs_to_id = [
                    x for x in contribs_by_id.values() if x.get("committee_id") == c_id
                ]
                if contribs_to_id and (
                    sum([x["contribution_receipt_amount"] for x in contribs_to_id])
                    == contrib["contribution_receipt_amount"]
                ):
                    # Remove the parent transaction
                    del contribs_by_id[contrib["transaction_id"]]
            else:
                transfer_match = re.search(r"FROM (.*?)(?:$| JFC)", description)
                if transfer_match:
                    from_source = [
                        x
                        for x in contribs_by_id.values()
                        if x["committee_name"] == transfer_match.group(1)
                    ]
                    if from_source:
                        from_source_sum = sum(
                            [x["contribution_receipt_amount"] for x in from_source]
                        )
                        to = [
                            x
                            for x in contribs_by_id.values()
                            if (
                                get_description(x)
                                and transfer_match.group(1) in get_description(x)
                            )
                        ]
                        to_sum = sum([x["contribution_receipt_amount"] for x in to])
                        if from_source_sum == to_sum:
                            # Remove the parent transaction
                            for from_contrib in from_source:
                                del contribs_by_id[from_contrib["transaction_id"]]
    return [attribute_earmarked(c) for c in list(contribs_by_id.values())]


def attribute_earmarked(contrib):
    # Try to attribute the contribution to a committee mentioned in the description
    description = get_description(contrib)
    if description:
        c_id = re.search(r"\((C\d+)\)", description)
        if c_id:
            return {
                **contrib,
                "committee_id": c_id.group(1),
                "committee_name": None,
                "candidate_ids": [],
                "committee_type": None,
                "committee_type_full": None,
                "designation": None,
                "designation_full": None,
                "party": None,
                "state": None,
            }
    return contrib


def process_contribution_group(group):
    if len(group) == 1:
        return [attribute_earmarked(group[0])]
    else:
        contrib_24t = []
        contrib_15 = []
        contrib_15e = []
        contrib_15x = []
        contrib_rest = []
        for contrib in group:
            receipt_type = contrib.get("receipt_type")
            if not receipt_type:
                contrib_rest.append(contrib)
            else:
                if receipt_type == "24T":
                    contrib_24t.append(contrib)
                elif receipt_type == "15":
                    contrib_15.append(contrib)
                elif receipt_type == "15E":
                    contrib_15e.append(contrib)
                elif receipt_type.startswith("15"):
                    contrib_15x.append(contrib)
                else:
                    contrib_rest.append(contrib)

        sum_24t = sum([x["contribution_receipt_amount"] for x in contrib_24t])
        sum_15 = sum([x["contribution_receipt_amount"] for x in contrib_15])
        sum_15e = sum([x["contribution_receipt_amount"] for x in contrib_15e])
        sum_15x = sum([x["contribution_receipt_amount"] for x in contrib_15x])
        sum_rest = sum([x["contribution_receipt_amount"] for x in contrib_rest])

        contribs_to_keep = []

        if sum_24t == sum_15 == sum_15e == sum_15x == sum_rest == 0:
            totals = {}
            for contrib in group:
                if contrib["committee_id"] not in totals:
                    totals[contrib["committee_id"]] = 0
                totals[contrib["committee_id"]] += contrib[
                    "contribution_receipt_amount"
                ]
                if all([x == 0 for x in totals.values()]):
                    # Just redesignation(s), can ignore
                    return []
            else:
                print("hmm")

        if not contrib_24t and not contrib_15 and not contrib_15e:
            # No earmarked contributions, these are just multiple contributions on the same day
            return contrib_15x + contrib_rest

        if contrib_24t:
            if sum_24t != sum_15 and sum_24t != sum_15e and sum_24t != sum_15x:
                contribs_to_keep.extend(dedupe_by_memo(group))
                return contribs_to_keep
            else:
                # Drop 24t contribution, fall through to 15 handling
                pass

        if contrib_15 or contrib_15e or contrib_15x:
            if sum_15e == 0 and sum_15 > 0 and sum_15 == sum_15x:
                contribs_to_keep.extend(contrib_15x + contrib_rest)
            elif sum_15 == 0 and sum_15e > 0 and sum_15e == sum_15x:
                contribs_to_keep.extend(contrib_15x + contrib_rest)
            elif not contrib_15x:
                if not contrib_15e:
                    contribs_to_keep.extend(contrib_15 + contrib_rest)
                elif not contrib_15:
                    contribs_to_keep.extend(contrib_15e + contrib_rest)
                else:
                    contribs_to_keep.extend(dedupe_by_memo(group))
                    return contribs_to_keep
            else:
                contribs_to_keep.extend(dedupe_by_memo(group))
                return contribs_to_keep
        else:
            print("uh oh")

        return contribs_to_keep


def process_individual_contributions(db):
    all_recipients = {}

    db.client.collection("rawIndividualContributions").stream()
    for doc in db.client.collection("rawIndividualContributions").stream():
        ind_id, ind = doc.id, doc.to_dict()
        contributions = ind["contributions"]

        grouped_by_date = {}
        for contrib in contributions:
            if contrib["contribution_receipt_date"] not in grouped_by_date:
                grouped_by_date[contrib["contribution_receipt_date"]] = []
            grouped_by_date[contrib["contribution_receipt_date"]].append(contrib)

        deduped = []
        for date, contribs in grouped_by_date.items():
            deduped.extend(process_contribution_group(contribs))

        db.client.collection("rawIndividualContributions").document(ind_id).set(
            {
                "tmp": deduped,
            },
            merge=True,
        )
