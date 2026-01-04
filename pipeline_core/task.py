from dataclasses import dataclass, field
from typing import Callable, List, Optional, Any


@dataclass
class Task:
    """
    Represents a single task in the pipeline.

    Attributes:
        name: Unique identifier for the task
        func: The function to execute
        depends_on: List of task names this task depends on
        inputs: List of Firestore collections this task reads from
        outputs: List of Firestore collections this task writes to
        description: Human-readable description of what the task does
    """

    name: str
    func: Callable
    depends_on: List[str] = field(default_factory=list)
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    description: Optional[str] = None

    def __post_init__(self):
        if self.description is None and self.func.__doc__:
            self.description = self.func.__doc__.strip().split("\n")[0]

    def execute(self, context) -> Any:
        """Execute the task function with the given context."""
        return self.func(context)

    def __repr__(self):
        deps = f", depends_on={self.depends_on}" if self.depends_on else ""
        return f"Task(name='{self.name}'{deps})"


def task(
    name: str,
    depends_on: Optional[List[str]] = None,
    inputs: Optional[List[str]] = None,
    outputs: Optional[List[str]] = None,
):
    """
    Decorator to register a function as a pipeline task.

    Usage:
        @task(
            name="process_contributions",
            depends_on=["fetch_contributions"],
            inputs=["rawContributions"],
            outputs=["contributions"]
        )
        def process_contributions(context):
            # Task implementation
            pass

    Args:
        name: Unique identifier for the task
        depends_on: List of task names this task depends on
        inputs: List of Firestore collections this task reads from
        outputs: List of Firestore collections this task writes to
    """

    def decorator(func: Callable) -> Task:
        from .registry import TaskRegistry

        task_obj = Task(
            name=name,
            func=func,
            depends_on=depends_on or [],
            inputs=inputs or [],
            outputs=outputs or [],
        )

        # Register the task
        registry = TaskRegistry.get_instance()
        registry.register(task_obj)

        return task_obj

    return decorator
