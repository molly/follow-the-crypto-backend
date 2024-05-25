from utils import FEC_fetch, pick


def hydrate_committees(db):
    for committee in db.committees.values():
        details_data = FEC_fetch(
            "committee details",
            "https://api.open.fec.gov/v1/committee/" + committee["id"],
        )
        if details_data and "results" in details_data and details_data["results"][0]:
            details = details_data["results"][0]
            picked = pick(
                details,
                [
                    "affiliated_committee_name",
                    "candidate_ids",
                    "committee_type",
                    "committee_type_full",
                    "cycles",
                    "designation",
                    "designation_full",
                    "first_f1_date",
                    "leadership_pac",
                    "organization_type",
                    "organization_type_full",
                    "party",
                    "party_full",
                    "party_type",
                    "party_type_full",
                    "sponsor_candidate_ids",
                    "website",
                ],
            )
            picked["fec_name"] = details["name"]
            committee_data = {**committee, **picked}

        totals_data = FEC_fetch(
            "committee totals",
            "https://api.open.fec.gov/v1/committee/{}/totals".format(committee["id"]),
            params={"cycle": 2024},
        )
        if totals_data and "results" in totals_data and totals_data["results"][0]:
            totals = totals_data["results"][0]
            committee_data.update(
                **pick(
                    totals,
                    [
                        "contributions",
                        "contribution_refunds",
                        "net_contributions",
                        "receipts",
                        "independent_expenditures",
                    ],
                ),
            )

        db.client.collection("committees").document(committee["id"]).set(committee_data)
