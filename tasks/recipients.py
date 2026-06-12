from pipeline_core.task import task
from recipients import summarize_recipients as summarize


@task(
    name="summarize_recipients",
    # Must run after update_companies_for_individuals, NOT just
    # process_company_contributions. The standalone process_company_contributions
    # task produces an intermediate companies state; update_companies_for_individuals
    # then re-runs it with individual data merged and is the FINAL writer of the
    # companies collection. Depending only on the early task made summarize_recipients
    # read pre-final company totals (e.g. before duplicate contributions were dropped),
    # so recipientDetails — and the beneficiaries / quid pro quo pages — served stale,
    # inflated figures.
    depends_on=["update_companies_for_individuals", "process_individual_contributions"],
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
