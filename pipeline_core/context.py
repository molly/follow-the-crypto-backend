from dataclasses import dataclass
from typing import Any


@dataclass
class TaskContext:
    """Context object passed to each task containing shared resources."""

    db: Any  # Database instance
    session: Any  # CachedSession instance
    verbose: bool = False

    def log(self, message: str):
        """Log a message if verbose mode is enabled."""
        if self.verbose:
            print(f"[TASK] {message}")
