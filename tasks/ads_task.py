from pipeline_core.task import task
from ads import get_ads


@task(
    name="fetch_ads",
    depends_on=["hydrate_committees"],
    outputs=["ads"],
)
def fetch_ads(context):
    """Fetch committee advertising data."""
    diff = get_ads(context.db, video_limit=getattr(context, "ad_video_limit", None))
    return {"ads_diff": diff}
