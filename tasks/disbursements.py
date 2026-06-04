from pipeline_core.task import task
from committee_disbursements import (
    summarize_transfers_by_party,
    update_committee_disbursements,
)


@task(
    name="fetch_committee_disbursements",
    depends_on=["hydrate_committees"],
    outputs=["disbursements"],
)
def fetch_committee_disbursements(context):
    """Fetch committee disbursements from FEC API."""
    diff = update_committee_disbursements(context.db, context.session)
    return {"disbursement_diff": diff}


@task(
    name="summarize_committee_transfers_by_party",
    depends_on=[
        "fetch_committee_disbursements",
        "summarize_recipients",
        # Reads recipient-reported transfers (Schedule A) from the contributions
        # docs to build the hybrid by-party breakdown, so those must be fresh.
        "process_committee_contributions",
    ],
    outputs=["committees"],
)
def summarize_committee_transfers_by_party(context):
    """Summarize each committee's transfers to other committees by party."""
    summarize_transfers_by_party(context.db, context.session)
    return {"status": "success"}
