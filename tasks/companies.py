from pipeline_core.task import task
from company_spending import update_spending_by_company
from process_company_contributions import process_company_contributions as process_comp


@task(
    name="fetch_company_spending",
    depends_on=["hydrate_committees"],
    outputs=["rawCompanySpending"],
)
def fetch_company_spending(context):
    """Fetch company spending data."""
    update_spending_by_company(context.db, context.session)
    return {"status": "success"}


@task(
    name="process_company_contributions",
    depends_on=["fetch_company_spending"],
    inputs=["rawCompanySpending"],
    outputs=["companies"],
)
def process_company_contributions(context):
    """Process company contributions."""
    new_recipient_committees = process_comp(context.db, context.session)
    return {"new_recipient_committees": new_recipient_committees}
