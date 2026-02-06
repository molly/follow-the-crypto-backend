import logging
import traceback
from typing import List, Optional, Dict, Any
from datetime import datetime

from .context import TaskContext
from .registry import TaskRegistry
from .state import StateTracker
from .task import Task


class PipelineOrchestrator:
    """
    Orchestrates the execution of pipeline tasks.
    Handles dependency resolution, state tracking, and error handling.
    """

    def __init__(self, db, session, registry: Optional[TaskRegistry] = None, verbose: bool = False):
        """
        Initialize the orchestrator.

        Args:
            db: Database instance
            session: CachedSession instance
            registry: TaskRegistry instance (uses singleton if not provided)
            verbose: Enable verbose logging
        """
        self.db = db
        self.session = session
        self.registry = registry or TaskRegistry.get_instance()
        self.state_tracker = StateTracker(db)
        self.verbose = verbose
        self.context = TaskContext(db=db, session=session, verbose=verbose)

    def build_execution_plan(
        self,
        task_names: Optional[List[str]] = None,
        force: bool = False,
        skip_deps: bool = False,
        skip_tasks: Optional[List[str]] = None,
    ) -> List[Task]:
        """
        Build an execution plan for tasks.

        Args:
            task_names: List of task names to execute. If None, executes all tasks.
            force: If True, force execution of all tasks even if already completed
            skip_deps: If True, skip dependencies and run only specified tasks
            skip_tasks: List of task names to exclude from execution

        Returns:
            List of tasks in execution order

        Raises:
            ValueError: If task names are invalid or dependencies can't be resolved
        """
        # Validate skip_tasks names
        if skip_tasks:
            for name in skip_tasks:
                if not self.registry.has_task(name):
                    raise ValueError(f"Unknown task to skip: '{name}'")

        if skip_deps and task_names:
            # Skip dependencies and run only specified tasks
            tasks = []
            for name in task_names:
                if self.registry.has_task(name):
                    tasks.append(self.registry.get_task(name))
                else:
                    raise ValueError(f"Unknown task: '{name}'")
        else:
            # Resolve dependencies to get tasks in execution order
            tasks = self.registry.resolve_dependencies(task_names)

        # If no specific tasks requested, filter out tasks marked run_by_default=False
        if task_names is None:
            tasks = [t for t in tasks if t.run_by_default]

        # Filter out tasks that don't need execution (unless force=True)
        if not force:
            tasks = [t for t in tasks if self.state_tracker.needs_execution(t, force)]

        # Remove explicitly skipped tasks
        if skip_tasks:
            skip_set = set(skip_tasks)
            tasks = [t for t in tasks if t.name not in skip_set]

        return tasks

    def execute(
        self,
        plan: List[Task],
        dry_run: bool = False,
        stop_on_failure: bool = True,
    ) -> Dict[str, Any]:
        """
        Execute a list of tasks in order.

        Args:
            plan: List of tasks to execute (in order)
            dry_run: If True, only print what would be executed
            stop_on_failure: If True, stop execution on first failure

        Returns:
            Dictionary with execution results:
            {
                "executed": [list of executed task names],
                "skipped": [list of skipped task names],
                "failed": [list of failed task names],
                "results": {task_name: result_data}
            }
        """
        executed = []
        skipped = []
        failed = []
        results = {}

        print(f"\n{'='*60}")
        print(f"Pipeline Execution Plan ({len(plan)} tasks)")
        print(f"{'='*60}")

        if dry_run:
            print("\n[DRY RUN MODE - No tasks will be executed]\n")
            for i, task in enumerate(plan, 1):
                deps = f" (depends on: {', '.join(task.depends_on)})" if task.depends_on else ""
                desc = f" - {task.description}" if task.description else ""
                print(f"{i}. {task.name}{deps}{desc}")
            print(f"\n{'='*60}\n")
            return {
                "executed": [],
                "skipped": [],
                "failed": [],
                "results": {},
            }

        # Execute tasks
        start_time = datetime.now()

        for i, task in enumerate(plan, 1):
            print(f"\n[{i}/{len(plan)}] Executing: {task.name}")
            if task.description:
                print(f"    {task.description}")

            try:
                # Mark task as started
                self.state_tracker.mark_started(task)

                # Execute the task
                task_start = datetime.now()
                result = task.execute(self.context)
                task_duration = (datetime.now() - task_start).total_seconds()

                # Mark task as completed
                self.state_tracker.mark_completed(task, result)

                executed.append(task.name)
                results[task.name] = result

                print(f"    ✓ Completed in {task_duration:.1f}s")

            except Exception as e:
                # Mark task as failed
                self.state_tracker.mark_failed(task, e)
                failed.append(task.name)

                print(f"    ✗ Failed: {e}")
                if self.verbose:
                    traceback.print_exc()

                logging.error(f"Task '{task.name}' failed: {e}")
                logging.debug(traceback.format_exc())

                if stop_on_failure:
                    print(f"\n{'='*60}")
                    print("Pipeline stopped due to task failure")
                    print(f"{'='*60}\n")
                    break

        total_duration = (datetime.now() - start_time).total_seconds()

        # Print summary
        print(f"\n{'='*60}")
        print("Pipeline Execution Summary")
        print(f"{'='*60}")
        print(f"Total duration: {total_duration:.1f}s")
        print(f"Executed: {len(executed)} tasks")
        print(f"Failed: {len(failed)} tasks")
        print(f"Skipped: {len(skipped)} tasks")

        if failed:
            print(f"\nFailed tasks:")
            for task_name in failed:
                print(f"  - {task_name}")

        print(f"{'='*60}\n")

        return {
            "executed": executed,
            "skipped": skipped,
            "failed": failed,
            "results": results,
        }

    def run(
        self,
        task_names: Optional[List[str]] = None,
        force: bool = False,
        dry_run: bool = False,
        stop_on_failure: bool = True,
        skip_deps: bool = False,
        skip_tasks: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Build and execute a pipeline.

        This is a convenience method that combines build_execution_plan and execute.

        Args:
            task_names: List of task names to execute. If None, executes all tasks.
            force: If True, force execution of all tasks
            dry_run: If True, only print what would be executed
            stop_on_failure: If True, stop execution on first failure
            skip_deps: If True, skip dependencies and run only specified tasks
            skip_tasks: List of task names to exclude from execution

        Returns:
            Dictionary with execution results
        """
        plan = self.build_execution_plan(task_names, force, skip_deps, skip_tasks)
        return self.execute(plan, dry_run, stop_on_failure)
