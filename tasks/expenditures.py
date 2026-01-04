from pipeline_core.task import task
from committee_expenditures import update_committee_expenditures
from process_committee_expenditures import process_expenditures as process_exp
from candidate_expenditures import update_candidates_expenditures


@task(
    name="fetch_committee_expenditures",
    depends_on=["hydrate_committees"],
    outputs=["rawExpenditures"],
)
def fetch_committee_expenditures(context):
    """Fetch raw committee expenditures from FEC API."""
    diff = update_committee_expenditures(context.db, context.session)
    return {"expenditure_diff": diff}


@task(
    name="process_expenditures",
    depends_on=["fetch_committee_expenditures"],
    inputs=["rawExpenditures"],
    outputs=["expenditures", "oppositionSpending"],
)
def process_expenditures(context):
    """Process committee expenditures and opposition spending."""
    new_opposition_spending = process_exp(context.db)
    return {"new_opposition_spending": new_opposition_spending}


@task(
    name="update_candidate_expenditures",
    depends_on=["process_expenditures"],
    inputs=["expenditures"],
)
def update_candidate_expenditures(context):
    """Group expenditures by candidate."""
    update_candidates_expenditures(context.db)
    return {"status": "success"}
