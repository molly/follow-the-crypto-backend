from pipeline_core.task import task
from candidate_trim import trim_candidates as trim
from candidate_images import get_candidates_without_images
from outside_spending import update_candidate_outside_spending


@task(
    name="trim_candidates",
    depends_on=["summarize_races"],
)
def trim_candidates(context):
    """Remove candidates without significant spending from race lists."""
    trim(context.db)
    return {"status": "success"}


@task(
    name="fetch_candidate_images",
    depends_on=["trim_candidates"],
)
def fetch_candidate_images(context):
    """Get list of candidates without images."""
    new_candidates = get_candidates_without_images(context.db)
    return {"new_candidates": new_candidates}


@task(
    name="update_outside_spending",
    depends_on=["process_expenditures", "update_race_details"],
    inputs=["expenditures", "raceDetails"],
)
def update_outside_spending(context):
    """Fetch outside spending data for candidates."""
    update_candidate_outside_spending(context.db, context.session)
    return {"status": "success"}
