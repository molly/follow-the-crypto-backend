from pipeline_core.task import task
from recipients import summarize_recipients as summarize


@task(
    name="summarize_recipients",
    depends_on=["process_individual_contributions", "process_company_contributions"],
    outputs=["allRecipients"],
)
def summarize_recipients(context):
    """Aggregate recipient data from individual and company contributions."""
    summarize(context.db)
    return {"status": "success"}
