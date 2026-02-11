from pipeline_core.task import task
from individuals import update_spending_by_individuals
from process_individual_contributions import process_individual_contributions as process_ind


@task(
    name="fetch_individual_spending",
    depends_on=["hydrate_committees"],
    outputs=["rawIndividualSpending"],
)
def fetch_individual_spending(context):
    """Fetch individual spending data."""
    update_spending_by_individuals(context.db, context.session)
    return {"status": "success"}


@task(
    name="process_individual_contributions",
    depends_on=["fetch_individual_spending"],
    inputs=["rawIndividualSpending"],
    outputs=["companies"],
)
def process_individual_contributions(context):
    """Process individual contributions."""
    new_recipient_committees = process_ind(context.db, context.session)
    return {"new_recipient_committees": new_recipient_committees}
