from pipeline_core.task import task
from process_recent_contributions import process_recent_contributions


@task(
    name="process_recent_contributions",
    depends_on=["process_individual_contributions", "process_company_contributions"],
    inputs=["individuals", "companies"],
    outputs=["contributions"],
)
def process_recent_contributions_task(context):
    """Generate the recent contributions snapshot for the home page feed."""
    process_recent_contributions(context.db)
    return {"status": "success"}
