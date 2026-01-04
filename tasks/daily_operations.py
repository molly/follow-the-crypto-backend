"""
Daily operation optimization tasks for the pipeline.

These tasks are designed for frequent runs and minimize redundant processing.
"""

from pipeline_core.task import task
import logging
import time


@task(
    name="daily_individual_update",
    depends_on=["hydrate_committees"],
    outputs=["individuals", "companies"],
)
def daily_individual_update(context, individual_ids=None, check_existing=True):
    """
    Optimized individual processing for daily operations.
    
    Only processes individuals that:
    1. Are specified in individual_ids, OR
    2. Don't have processed data yet (when check_existing=True)
    
    Args:
        individual_ids: Specific individuals to process
        check_existing: Whether to check for individuals missing processed data
    """
    from commands.batch_daily import process_pending_individuals
    
    if individual_ids:\n        logging.info(f\"Daily update for specific individuals: {individual_ids}\")\n        result = process_pending_individuals(context.db, individual_ids)\n    elif check_existing:\n        logging.info(\"Daily update: checking for individuals without processed data\")\n        result = process_pending_individuals(context.db)\n    else:\n        logging.info(\"Daily update: no individuals specified and check_existing=False\")\n        return {\"status\": \"skipped\", \"reason\": \"no_individuals_specified\"}\n    \n    return {\n        \"status\": \"completed\",\n        \"task_type\": \"daily_individual_update\",\n        **result\n    }\n\n\n@task(\n    name=\"daily_incremental_pipeline\",\n    depends_on=[\"daily_individual_update\"],\n    outputs=[\"individuals\", \"companies\", \"allRecipients\"],\n)\ndef daily_incremental_pipeline(context, max_runtime_minutes=30):\n    \"\"\"\n    Run a daily incremental pipeline that focuses on new/changed data.\n    \n    This task:\n    1. Processes any individuals without data\n    2. Updates only affected companies\n    3. Updates recipient summaries\n    4. Has a maximum runtime limit\n    \n    Args:\n        max_runtime_minutes: Maximum time to spend on processing\n    \"\"\"\n    start_time = time.time()\n    max_runtime_seconds = max_runtime_minutes * 60\n    \n    logging.info(f\"Starting daily incremental pipeline (max runtime: {max_runtime_minutes} minutes)\")\n    \n    operations = []\n    \n    # Check if we have any unprocessed individuals\n    unprocessed = []\n    for ind_id in context.db.individuals.keys():\n        if time.time() - start_time > max_runtime_seconds:\n            logging.warning(\"Approaching max runtime, stopping individual checks\")\n            break\n            \n        existing_data = context.db.client.collection(\"individuals\").document(ind_id).get()\n        if not existing_data.exists or not existing_data.to_dict().get(\"contributions\"):\n            unprocessed.append(ind_id)\n    \n    if unprocessed:\n        logging.info(f\"Found {len(unprocessed)} unprocessed individuals\")\n        from commands.batch_daily import process_pending_individuals\n        \n        # Process in batches if there are many\n        batch_size = 5  # Limit batch size for daily operations\n        processed_count = 0\n        \n        for i in range(0, len(unprocessed), batch_size):\n            if time.time() - start_time > max_runtime_seconds:\n                logging.warning(f\"Max runtime reached, processed {processed_count}/{len(unprocessed)} individuals\")\n                break\n                \n            batch = unprocessed[i:i + batch_size]\n            logging.info(f\"Processing batch {i//batch_size + 1}: {batch}\")\n            \n            batch_result = process_pending_individuals(context.db, batch)\n            operations.append({\n                \"type\": \"individual_batch\",\n                \"batch\": batch,\n                \"result\": batch_result\n            })\n            \n            processed_count += len(batch)\n    \n    # Update recipients summary if we processed anything\n    if operations:\n        logging.info(\"Updating recipient summaries\")\n        from recipients import summarize_recipients\n        summarize_recipients(context.db)\n        operations.append({\"type\": \"recipients_summary\", \"status\": \"completed\"})\n    \n    elapsed_time = time.time() - start_time\n    \n    return {\n        \"status\": \"completed\",\n        \"operations\": operations,\n        \"unprocessed_found\": len(unprocessed) if 'unprocessed' in locals() else 0,\n        \"processed_count\": processed_count if 'processed_count' in locals() else 0,\n        \"elapsed_time_seconds\": round(elapsed_time, 2),\n        \"within_time_limit\": elapsed_time <= max_runtime_seconds,\n        \"optimization\": \"daily_incremental\"\n    }\n\n\n@task(\n    name=\"daily_quick_check\",\n    depends_on=[],\n    outputs=[],\n)\ndef daily_quick_check(context):\n    \"\"\"\n    Quick status check for daily operations.\n    Shows what needs processing without actually processing.\n    \"\"\"\n    logging.info(\"Running daily quick check\")\n    \n    # Count individuals without processed data\n    unprocessed_individuals = 0\n    total_individuals = len(context.db.individuals)\n    \n    for ind_id in context.db.individuals.keys():\n        existing_data = context.db.client.collection(\"individuals\").document(ind_id).get()\n        if not existing_data.exists or not existing_data.to_dict().get(\"contributions\"):\n            unprocessed_individuals += 1\n    \n    # Get recent additions (could check last modified dates)\n    # For now, just report counts\n    \n    return {\n        \"status\": \"completed\",\n        \"total_individuals\": total_individuals,\n        \"unprocessed_individuals\": unprocessed_individuals,\n        \"processed_individuals\": total_individuals - unprocessed_individuals,\n        \"needs_processing\": unprocessed_individuals > 0,\n        \"check_type\": \"daily_status\"\n    }