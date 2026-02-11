from pipeline_core.task import task
from committee_disbursements import update_committee_disbursements


@task(
    name="fetch_committee_disbursements",
    depends_on=["hydrate_committees"],
    outputs=["disbursements"],
)
def fetch_committee_disbursements(context):
    """Fetch committee disbursements from FEC API."""
    diff = update_committee_disbursements(context.db, context.session)
    return {"disbursement_diff": diff}
