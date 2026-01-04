from typing import Dict, List, Optional, Set
import logging


class TaskRegistry:
    """
    Singleton registry for all pipeline tasks.
    Manages task registration and dependency resolution.
    """

    _instance: Optional["TaskRegistry"] = None

    def __init__(self):
        self._tasks: Dict[str, "Task"] = {}  # noqa: F821

    @classmethod
    def get_instance(cls) -> "TaskRegistry":
        """Get the singleton instance of the registry."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None

    def register(self, task: "Task"):  # noqa: F821
        """
        Register a task in the registry.

        Args:
            task: The task to register

        Raises:
            ValueError: If a task with the same name is already registered
        """
        if task.name in self._tasks:
            raise ValueError(
                f"Task '{task.name}' is already registered. "
                f"Task names must be unique."
            )

        self._tasks[task.name] = task
        logging.debug(f"Registered task: {task.name}")

    def get_task(self, name: str) -> "Task":  # noqa: F821
        """
        Get a task by name.

        Args:
            name: The name of the task

        Returns:
            The task object

        Raises:
            KeyError: If the task is not found
        """
        if name not in self._tasks:
            raise KeyError(f"Task '{name}' not found in registry")
        return self._tasks[name]

    def get_all_tasks(self) -> List["Task"]:  # noqa: F821
        """Get all registered tasks."""
        return list(self._tasks.values())

    def has_task(self, name: str) -> bool:
        """Check if a task is registered."""
        return name in self._tasks

    def resolve_dependencies(self, task_names: Optional[List[str]] = None) -> List["Task"]:  # noqa: F821
        """
        Resolve task dependencies and return tasks in execution order.

        Args:
            task_names: List of task names to execute. If None, executes all tasks.

        Returns:
            List of tasks in topologically sorted order

        Raises:
            ValueError: If circular dependencies are detected or unknown tasks are referenced
        """
        if task_names is None:
            # Execute all tasks
            tasks_to_execute = set(self._tasks.keys())
        else:
            # Resolve all dependencies for specified tasks
            tasks_to_execute = set()
            for name in task_names:
                self._add_task_and_dependencies(name, tasks_to_execute)

        # Perform topological sort
        sorted_tasks = self._topological_sort(tasks_to_execute)
        return sorted_tasks

    def _add_task_and_dependencies(self, task_name: str, result_set: Set[str]):
        """Recursively add a task and all its dependencies to the result set."""
        if task_name not in self._tasks:
            raise ValueError(f"Unknown task: '{task_name}'")

        if task_name in result_set:
            return

        task = self._tasks[task_name]
        for dep in task.depends_on:
            self._add_task_and_dependencies(dep, result_set)

        result_set.add(task_name)

    def _topological_sort(self, task_names: Set[str]) -> List["Task"]:  # noqa: F821
        """
        Perform topological sort on tasks using Kahn's algorithm.

        Args:
            task_names: Set of task names to sort

        Returns:
            List of Task objects in execution order

        Raises:
            ValueError: If circular dependencies are detected
        """
        # Build in-degree map
        in_degree = {name: 0 for name in task_names}
        adjacency = {name: [] for name in task_names}

        for name in task_names:
            task = self._tasks[name]
            for dep in task.depends_on:
                if dep in task_names:
                    adjacency[dep].append(name)
                    in_degree[name] += 1

        # Find all tasks with no dependencies
        queue = [name for name in task_names if in_degree[name] == 0]
        result = []

        while queue:
            # Sort queue for deterministic ordering
            queue.sort()
            current = queue.pop(0)
            result.append(self._tasks[current])

            # Reduce in-degree for dependent tasks
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(task_names):
            # Circular dependency detected
            remaining = [name for name in task_names if name not in [t.name for t in result]]
            raise ValueError(
                f"Circular dependency detected among tasks: {remaining}. "
                f"Please check task dependencies."
            )

        return result

    def validate(self):
        """
        Validate all registered tasks.

        Raises:
            ValueError: If validation fails (e.g., unknown dependencies)
        """
        for task in self._tasks.values():
            for dep in task.depends_on:
                if dep not in self._tasks:
                    raise ValueError(
                        f"Task '{task.name}' depends on unknown task '{dep}'"
                    )

        # Check for circular dependencies by attempting to resolve all tasks
        try:
            self.resolve_dependencies()
        except ValueError as e:
            raise ValueError(f"Task validation failed: {e}")

    def __repr__(self):
        return f"TaskRegistry({len(self._tasks)} tasks)"
