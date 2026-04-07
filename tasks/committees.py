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

    def empty_totals():
        return {
            "receipts": 0,
            "expenditures": 0,
            "disbursements": 0,
            "cash_on_hand": 0,
            "claimed_committed": 0,
        }

    combined_committee_totals = {
        "all": empty_totals(),
        "crypto": empty_totals(),
        "ai": empty_totals(),
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
                            "net_contributions",
                            "receipts",
                            "independent_expenditures",
                        ],
                    ),
                )
                sector_keys = ["all"]
                committee_sector = committee.get("sector")
                if committee_sector in combined_committee_totals:
                    sector_keys.append(committee_sector)
                for key in sector_keys:
                    combined_committee_totals[key]["receipts"] += totals["receipts"]
                    combined_committee_totals[key]["expenditures"] += totals[
                        "independent_expenditures"
                    ]
                    combined_committee_totals[key]["disbursements"] += totals["disbursements"]

            # Fetch cash on hand from the 2024 cycle to get EOY 2024 balance,
            # avoiding double-counting 2025 contributions.
            # Newly formed committees return None here, which is fine — they had $0.
            cash_on_hand = 0
            cash_on_hand_data = FEC_fetch(
                session,
                "committee EOY 2024 cash on hand",
                "https://api.open.fec.gov/v1/committee/{}/totals".format(
                    committee["id"]
                ),
                params={"cycle": 2024},
            )
            if (
                cash_on_hand_data
                and "results" in cash_on_hand_data
                and len(cash_on_hand_data["results"])
                and cash_on_hand_data["results"][0]
            ):
                cash_on_hand = cash_on_hand_data["results"][0].get(
                    "last_cash_on_hand_end_period", 0
                )
            committee_data["last_cash_on_hand_end_period"] = cash_on_hand
            sector_keys = ["all"]
            committee_sector = committee.get("sector")
            if committee_sector in combined_committee_totals:
                sector_keys.append(committee_sector)
            for key in sector_keys:
                combined_committee_totals[key]["cash_on_hand"] += cash_on_hand
                combined_committee_totals[key]["claimed_committed"] += committee.get(
                    "claimedCommitted", 0
                )

            db.client.collection("committees").document(committee["id"]).set(
                committee_data
            )
            committees_processed += 1

    for sector_key, totals_dict in combined_committee_totals.items():
        for field in totals_dict:
            totals_dict[field] = round(totals_dict[field], 2)
    db.client.collection("totals").document("committees").set(combined_committee_totals)

    return {
        "committees_processed": committees_processed,
        "totals": combined_committee_totals,
    }
