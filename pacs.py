from utils import FEC_fetch, pick

FIELDS = [
    "committee_id",
    "committee_name",
    "committee_type",
    "committee_type_full",
    "committee_designation",
    "committee_designation_full",
    "receipts",
    "disbursements",
    "independent_expenditures",
    "last_cash_on_hand_end_period",
]


def get_pac_data(pac, db):
    pac_data = pick(pac, FIELDS)
    if pac_data["committee_id"] in db.all_committees:
        pac_data["description"] = db.all_committees[pac_data["committee_id"]]
    pac_data["is_crypto"] = pac_data["committee_id"] in db.committees.keys()
    return pac_data


def get_top_raised_by_type(db, committee_type=None):
    params = {"cycle": 2024, "sort": "-receipts", "per_page": 50}
    if committee_type:
        params["committee_type"] = committee_type
    top_raised_data = FEC_fetch(
        "top PACs by amount raised",
        "https://api.open.fec.gov/v1/totals/pac-party",
        params=params,
    )
    if top_raised_data and "results" in top_raised_data:
        top_raised = top_raised_data["results"]
        return [get_pac_data(pac, db) for pac in top_raised]
    return []


def get_top_raised_pacs(db):
    all_pacs = get_top_raised_by_type(db)
    super_pacs = get_top_raised_by_type(db, "O")
    db.client.collection("allCommittees").document("allPacs").set(
        {"by_receipts": all_pacs}
    )
    db.client.collection("allCommittees").document("superPacs").set(
        {"by_receipts": super_pacs}
    )
