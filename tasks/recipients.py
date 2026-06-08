from pipeline_core.task import task
from recipients import summarize_recipients as summarize


@task(
    name="summarize_recipients",
    depends_on=["process_individual_contributions", "process_company_contributions"],
    # Reads the contribution sources directly, so declare them as inputs: with no
    # inputs the cache could only re-run this via a dependency's timestamp, letting
    # recipientDetails go stale when companies/individuals changed on their own.
    inputs=["companies", "individuals"],
    outputs=["recipientDetails", "allRecipients"],
)
def summarize_recipients(context):
    """Aggregate recipient data from individual and company contributions."""
    summarize(context.db)
    return {"status": "success"}
