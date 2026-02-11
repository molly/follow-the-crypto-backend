from pipeline_core.task import task
from utils import FEC_fetch, pick


@task(
    name="hydrate_committees",
    depends_on=[],
    outputs=["committees", "totals"],
)
def hydrate_committees(context):
    """Fetch committee details and totals from FEC API."""
    db = context.db
    session = context.session

    combined_committee_totals = {
        "receipts": 0,
        "expenditures": 0,
        "disbursements": 0,
        "cash_on_hand": 0,
    }
    committees_processed = 0

    for committee in db.committees.values():
        details_data = FEC_fetch(
            session,
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
                session,
                "committee totals",
                "https://api.open.fec.gov/v1/committee/{}/totals".format(
                    committee["id"]
                ),
                params={"cycle": 2026},
            )
            if (
                totals_data
                and "results" in totals_data
                and len(totals_data["results"])
                and totals_data["results"][0]
            ):
                totals = totals_data["results"][0]
                committee_data.update(
                    **pick(
                        totals,
                        [
                            "contributions",
                            "contribution_refunds",
                            "disbursements",
                            "last_cash_on_hand_end_period",
                            "net_contributions",
                            "receipts",
                            "independent_expenditures",
                        ],
                    ),
                )
                combined_committee_totals["receipts"] += totals["receipts"]
                combined_committee_totals["expenditures"] += totals[
                    "independent_expenditures"
                ]
                combined_committee_totals["disbursements"] += totals["disbursements"]
                combined_committee_totals["cash_on_hand"] += totals.get(
                    "last_cash_on_hand_end_period", 0
                )

            db.client.collection("committees").document(committee["id"]).set(
                committee_data
            )
            committees_processed += 1

    combined_committee_totals["receipts"] = round(
        combined_committee_totals["receipts"], 2
    )
    combined_committee_totals["expenditures"] = round(
        combined_committee_totals["expenditures"], 2
    )
    combined_committee_totals["disbursements"] = round(
        combined_committee_totals["disbursements"], 2
    )
    combined_committee_totals["cash_on_hand"] = round(
        combined_committee_totals["cash_on_hand"], 2
    )
    db.client.collection("totals").document("committees").set(combined_committee_totals)

    return {
        "committees_processed": committees_processed,
        "totals": combined_committee_totals,
    }
