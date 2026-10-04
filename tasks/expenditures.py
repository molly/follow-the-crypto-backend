from pipeline_core.task import task
from committee_expenditures import update_committee_expenditures
from process_committee_expenditures import process_expenditures as process_exp
from candidate_expenditures import update_candidates_expenditures
from process_company_state_spending import compute_company_state_spending


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
    # raceDetails: build_roster_maps reads the rosters to attribute each
    # expenditure to a seat, to resolve the candidate's display name, and to
    # date-match its sub-race, so adding a missing race (or fixing a roster
    # name) has to re-run this or the spending stays orphaned.
    inputs=["rawExpenditures", "raceDetails"],
    outputs=["expenditures", "oppositionSpending"],
)
def process_expenditures(context):
    """Process committee expenditures and opposition spending."""
    new_opposition_spending = process_exp(context.db)
    return {"new_opposition_spending": new_opposition_spending}


@task(
    name="compute_company_state_spending",
    depends_on=["process_expenditures", "process_company_contributions"],
    inputs=["expenditures", "companies", "raceDetails", "allRecipients"],
    outputs=["expenditures"],
)
def compute_company_state_spending_task(context):
    """Compute company spending by state from candidate associations."""
    compute_company_state_spending(context.db)
    return {"status": "success"}


@task(
    name="update_candidate_expenditures",
    depends_on=["prune_race_details"],
    inputs=["expenditures", "raceSummaries", "raceDetails"],
)
def update_candidate_expenditures(context):
    """Group expenditures by candidate."""
    update_candidates_expenditures(context.db)
    return {"status": "success"}
