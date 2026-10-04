from dataclasses import dataclass
from typing import Any


@dataclass
class TaskContext:
    """Context object passed to each task containing shared resources."""

    db: Any  # Database instance
    session: Any  # requests.Session / CachedSession instance
    verbose: bool = False
    # When True, fetch_committee_contributions does a full re-fetch + overwrite instead of an
    # incremental update. Set from the --full-contributions CLI flag.
    full_contributions: bool = False
    # When True, ALL incremental FEC-fetch tasks (contributions, individuals, company, etc.) do a
    # full re-fetch + overwrite. Set from the --full-fetch CLI flag; use for periodic reconciles.
    full_fetch: bool = False
    # Cap on how many ad videos fetch_ads rehosts in a single run. The Ad
    # Transparency Center throttles hard, so a large backfill takes a while;
    # this lets a run be capped. None means no cap. Set from --ad-video-limit.
    ad_video_limit: int = None

    def log(self, message: str):
        """Log a message if verbose mode is enabled."""
        if self.verbose:
            print(f"[TASK] {message}")
