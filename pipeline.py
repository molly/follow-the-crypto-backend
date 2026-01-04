#!/usr/bin/env python3
"""
Pipeline orchestration script for Follow the Crypto data pipeline.

Usage:
    python pipeline.py                          # Run all tasks (skip completed)
    python pipeline.py --force                  # Force re-run all tasks
    python pipeline.py --tasks task1,task2      # Run specific tasks and their dependencies
    python pipeline.py --dry-run                # Show execution plan without running
    python pipeline.py --clear-cache            # Clear HTTP cache before running
    python pipeline.py --verbose                # Enable verbose logging
"""

import argparse
import logging
import sys

import google.cloud.logging
from requests_cache import CachedSession

from Database import Database
from pipeline_core import PipelineOrchestrator, TaskRegistry

# Import all task modules to trigger registration
import tasks


def setup_logging(verbose: bool = False):
    """Set up logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Set up Google Cloud Logging
    try:
        client = google.cloud.logging.Client()
        client.setup_logging()
    except Exception as e:
        logging.warning(f"Could not set up Google Cloud Logging: {e}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Run the Follow the Crypto data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    Run all tasks (skip completed)
  %(prog)s --force                            Force re-run all tasks
  %(prog)s --tasks summarize_races            Run specific task and dependencies
  %(prog)s --tasks fetch_ads,process_contribs Run multiple tasks
  %(prog)s --dry-run                          Show what would run
  %(prog)s --list-tasks                       List all available tasks
  %(prog)s --clear-cache --force              Clear cache and re-run everything
        """,
    )

    parser.add_argument(
        "--tasks",
        type=str,
        help="Comma-separated list of task names to run (with dependencies)",
    )

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-run all tasks, ignoring completion state",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show execution plan without running tasks",
    )

    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear HTTP request cache before running",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="List all available tasks and exit",
    )

    parser.add_argument(
        "--continue-on-failure",
        action="store_true",
        help="Continue running other tasks if one fails",
    )

    parser.add_argument(
        "--individual-ids",
        type=str,
        help="Comma-separated list of individual IDs to process (for individual-related tasks)",
    )
        help="Continue execution even if a task fails",
    )

    return parser.parse_args()


def list_tasks(registry: TaskRegistry):
    """Print all available tasks."""
    tasks = registry.get_all_tasks()

    print("\nAvailable Tasks:")
    print("=" * 80)

    for task in sorted(tasks, key=lambda t: t.name):
        deps = f" (depends on: {', '.join(task.depends_on)})" if task.depends_on else ""
        desc = f"\n    {task.description}" if task.description else ""
        print(f"\n{task.name}{deps}{desc}")

    print("\n" + "=" * 80)
    print(f"\nTotal: {len(tasks)} tasks\n")


def main():
    """Main entry point for the pipeline."""
    args = parse_args()

    # Set up logging
    setup_logging(args.verbose)

    # Get registry and list tasks if requested
    registry = TaskRegistry.get_instance()

    if args.list_tasks:
        list_tasks(registry)
        return 0

    print("\n" + "=" * 80)
    print("Follow the Crypto Data Pipeline")
    print("=" * 80 + "\n")

    # Initialize shared resources
    print("Initializing database connection...")
    try:
        db = Database()
        db.get_constants()
        print("✓ Database connected and constants loaded")
    except Exception as e:
        print(f"✗ Failed to initialize database: {e}")
        return 1

    print("Initializing HTTP cache...")
    session = CachedSession("cache", backend="filesystem")
    if args.clear_cache:
        print("Clearing cache...")
        session.cache.clear()
        print("✓ Cache cleared")
    print("✓ HTTP cache ready")

    # Parse task names if provided
    task_names = None
    if args.tasks:
        task_names = [t.strip() for t in args.tasks.split(",")]
        print(f"\nRequested tasks: {', '.join(task_names)}")

    # Parse individual IDs if provided
    individual_ids = None
    if args.individual_ids:
        individual_ids = [i.strip() for i in args.individual_ids.split(",")]
        print(f"\nTarget individuals: {', '.join(individual_ids)}")

    # Validate registry
    try:
        registry.validate()
        print(f"\n✓ Task registry validated ({len(registry.get_all_tasks())} tasks)")
    except ValueError as e:
        print(f"\n✗ Task registry validation failed: {e}")
        return 1

    # Create orchestrator and run pipeline
    orchestrator = PipelineOrchestrator(
        db=db,
        session=session,
        registry=registry,
        verbose=args.verbose,
    )

    try:
        results = orchestrator.run(
            task_names=task_names,
            force=args.force,
            dry_run=args.dry_run,
            stop_on_failure=not args.continue_on_failure,
            individual_ids=individual_ids,  # Pass individual IDs to orchestrator
        )

        # Determine exit code based on results
        if results["failed"]:
            print("\n⚠ Pipeline completed with failures")
            return 1
        elif args.dry_run:
            print("\n✓ Dry run completed")
            return 0
        else:
            print("\n✓ Pipeline completed successfully")
            return 0

    except KeyboardInterrupt:
        print("\n\n⚠ Pipeline interrupted by user")
        return 130
    except Exception as e:
        print(f"\n\n✗ Pipeline failed with error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
