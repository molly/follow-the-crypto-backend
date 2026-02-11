from pipeline_core.task import task
from pacs import get_top_raised_pacs


@task(
    name="get_top_pacs",
    depends_on=["process_committee_contributions"],
    inputs=["contributions"],
)
def get_top_pacs(context):
    """Get top fundraising PACs."""
    get_top_raised_pacs(context.db, context.session)
    return {"status": "success"}
