import logging
from datetime import datetime
from typing import Optional, Dict, Any
from google.cloud import firestore


class StateTracker:
    """
    Tracks pipeline task execution state in Firestore.
    Enables resume functionality and smart skipping of completed tasks.
    """

    COLLECTION_NAME = "_pipeline_state"

    def __init__(self, db):
        """
        Initialize the state tracker.

        Args:
            db: Database instance with Firestore client
        """
        self.db = db
        self.collection = db.client.collection(self.COLLECTION_NAME)

    def needs_execution(self, task: "Task", force: bool = False) -> bool:  # noqa: F821
        """
        Check if a task needs to be executed.

        Args:
            task: The task to check
            force: If True, always return True (force execution)

        Returns:
            True if the task should be executed, False if it can be skipped
        """
        if force:
            return True

        state = self.get_state(task.name)

        if not state:
            # No previous execution
            logging.debug(f"Task '{task.name}' has no previous execution state")
            return True

        if state.get("status") == "failed":
            # Previous execution failed
            logging.debug(f"Task '{task.name}' previously failed, re-running")
            return True

        if state.get("status") != "completed":
            # Task not completed
            return True

        # Check if input collections have been modified since last completion
        completed_at = state.get("completed_at")
        if completed_at and task.inputs:
            for input_collection in task.inputs:
                if self._collection_modified_since(input_collection, completed_at):
                    logging.debug(
                        f"Task '{task.name}' input collection '{input_collection}' "
                        f"was modified, re-running"
                    )
                    return True

        # Task completed and inputs haven't changed
        logging.info(f"Skipping task '{task.name}' (already completed, inputs unchanged)")
        return False

    def _collection_modified_since(
        self, collection_name: str, timestamp: datetime
    ) -> bool:
        """
        Check if a Firestore collection has been modified since a given timestamp.

        This is a simple heuristic - we check if any document's update time
        is newer than the given timestamp. This requires documents to have
        an update timestamp field.

        Args:
            collection_name: Name of the Firestore collection
            timestamp: Timestamp to check against

        Returns:
            True if collection was modified, False otherwise
        """
        try:
            # Query for any documents updated after the timestamp
            # Note: This assumes documents have an 'updated_at' field
            # If not present, we assume the collection was modified
            query = (
                self.db.client.collection(collection_name)
                .where("updated_at", ">", timestamp)
                .limit(1)
            )
            docs = list(query.stream())
            return len(docs) > 0
        except Exception as e:
            # If we can't determine, assume it was modified
            logging.warning(
                f"Could not check modification time for collection '{collection_name}': {e}"
            )
            return True

    def get_state(self, task_name: str) -> Optional[Dict[str, Any]]:
        """
        Get the execution state for a task.

        Args:
            task_name: Name of the task

        Returns:
            Dictionary with state information, or None if no state exists
        """
        try:
            doc = self.collection.document(task_name).get()
            if doc.exists:
                return doc.to_dict()
            return None
        except Exception as e:
            logging.error(f"Error getting state for task '{task_name}': {e}")
            return None

    def mark_started(self, task: "Task"):  # noqa: F821
        """
        Mark a task as started.

        Args:
            task: The task that started
        """
        try:
            self.collection.document(task.name).set(
                {
                    "status": "running",
                    "started_at": firestore.SERVER_TIMESTAMP,
                    "task_name": task.name,
                    "description": task.description or "",
                },
                merge=True,
            )
            logging.debug(f"Marked task '{task.name}' as started")
        except Exception as e:
            logging.error(f"Error marking task '{task.name}' as started: {e}")

    def mark_completed(self, task: "Task", result: Any = None):  # noqa: F821
        """
        Mark a task as completed.

        Args:
            task: The task that completed
            result: Optional result data from the task
        """
        try:
            data = {
                "status": "completed",
                "completed_at": firestore.SERVER_TIMESTAMP,
                "task_name": task.name,
            }

            if result is not None:
                data["result"] = result

            self.collection.document(task.name).set(data, merge=True)
            logging.debug(f"Marked task '{task.name}' as completed")
        except Exception as e:
            logging.error(f"Error marking task '{task.name}' as completed: {e}")

    def mark_failed(self, task: "Task", error: Exception):  # noqa: F821
        """
        Mark a task as failed.

        Args:
            task: The task that failed
            error: The exception that caused the failure
        """
        try:
            self.collection.document(task.name).set(
                {
                    "status": "failed",
                    "failed_at": firestore.SERVER_TIMESTAMP,
                    "task_name": task.name,
                    "error": str(error),
                    "error_type": type(error).__name__,
                },
                merge=True,
            )
            logging.debug(f"Marked task '{task.name}' as failed")
        except Exception as e:
            logging.error(f"Error marking task '{task.name}' as failed: {e}")

    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """
        Get execution state for all tasks.

        Returns:
            Dictionary mapping task names to their state
        """
        states = {}
        try:
            docs = self.collection.stream()
            for doc in docs:
                states[doc.id] = doc.to_dict()
        except Exception as e:
            logging.error(f"Error getting all states: {e}")
        return states

    def clear_state(self, task_name: Optional[str] = None):
        """
        Clear execution state.

        Args:
            task_name: If provided, clear state for this task only.
                      If None, clear all state.
        """
        try:
            if task_name:
                self.collection.document(task_name).delete()
                logging.info(f"Cleared state for task '{task_name}'")
            else:
                docs = self.collection.stream()
                for doc in docs:
                    doc.reference.delete()
                logging.info("Cleared all task state")
        except Exception as e:
            logging.error(f"Error clearing state: {e}")
