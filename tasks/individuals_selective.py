"""
Enhanced individual processing tasks that support selective updates.
"""

from pipeline_core.task import task
from individuals import update_spending_by_individuals
from process_individual_contributions import process_individual_contributions as process_ind
import logging


@task(
    name="fetch_individual_spending_selective",
    depends_on=["hydrate_committees"],
    outputs=["rawIndividualSpending"],
)
def fetch_individual_spending_selective(context, individual_ids=None):
    """
    Fetch individual spending data for specific individuals or all.
    
    Args:
        individual_ids: List of individual IDs to process, or None for all
    """
    if individual_ids:
        logging.info(f"Fetching spending for specific individuals: {individual_ids}")
        
        # Temporarily filter individuals to only requested ones
        original_individuals = context.db.individuals.copy()
        context.db.individuals = {
            id: individual for id, individual in original_individuals.items()
            if id in individual_ids
        }
        
        try:
            new_contributions = update_spending_by_individuals(context.db, context.session)
            return {
                "status": "success",
                "processed_individuals": individual_ids,
                "new_contributions_count": len(new_contributions)
            }
        finally:
            # Restore full individuals list
            context.db.individuals = original_individuals
    else:
        # Process all individuals (existing behavior)
        update_spending_by_individuals(context.db, context.session)
        return {"status": "success", "processed_individuals": "all"}


@task(
    name="process_individual_contributions_selective",
    depends_on=["fetch_individual_spending_selective"],
    inputs=["rawIndividualSpending"],
    outputs=["companies"],
)
def process_individual_contributions_selective(context, individual_ids=None):
    """
    Process individual contributions for specific individuals or all.
    
    Args:
        individual_ids: List of individual IDs to process, or None for all
    """
    if individual_ids:
        logging.info(f"Processing contributions for specific individuals: {individual_ids}")
    
    # The process_ind function already processes all available raw data,
    # so we don't need to filter here - it will process whatever was fetched
    new_recipient_committees = process_ind(context.db, context.session)
    
    return {
        "new_recipient_committees": new_recipient_committees,
        "processed_individuals": individual_ids if individual_ids else "all"
    }


@task(
    name="add_and_process_individual",
    depends_on=["hydrate_committees"],
    outputs=["rawIndividualSpending", "companies"],
)
def add_and_process_individual(context, individual_id, individual_data):
    """
    Add a new individual and immediately fetch and process their data.
    
    Args:
        individual_id: Unique identifier for the individual
        individual_data: Dictionary containing individual details (should include 'id' field)
    """
    logging.info(f"Adding and processing new individual: {individual_id}")
    
    # Ensure the individual_data has the required 'id' field
    if "id" not in individual_data:
        individual_data["id"] = individual_id
    
    # Add to constants collection
    current_individuals = context.db.individuals.copy()
    current_individuals[individual_id] = individual_data
    
    # Update Firestore
    context.db.client.collection("constants").document("individuals").set(current_individuals)
    
    # Update local cache
    context.db.individuals = current_individuals
    
    # Now fetch and process just this individual
    fetch_result = fetch_individual_spending_selective(context, [individual_id])
    process_result = process_individual_contributions_selective(context, [individual_id])
    
    return {
        "individual_id": individual_id,
        "added": True,
        "fetch_result": fetch_result,
        "process_result": process_result
    }