from pipeline_core.task import task
from fetch_committee_contributions import update_committee_contributions
from process_committee_contributions import process_committee_contributions as process_contribs


@task(
    name="fetch_committee_contributions",
    depends_on=["hydrate_committees"],
    outputs=["rawContributions"],
)
def fetch_committee_contributions(context):
    """Fetch raw committee contributions from FEC API."""
    new_contributions = update_committee_contributions(context.db, context.session)
    return {"new_contributions_count": len(new_contributions)}


@task(
    name="process_committee_contributions",
    depends_on=["fetch_committee_contributions"],
    inputs=["rawContributions"],
    outputs=["contributions"],
)
def process_committee_contributions(context):
    """Process and aggregate committee contributions."""
    process_contribs(context.db)
    return {"status": "success"}
