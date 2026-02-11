"""
Task that processes company dependencies when individuals are added/updated.

This ensures that when individual data changes, all affected company 
aggregations are properly updated.
"""

from pipeline_core.task import task
from company_spending import update_spending_by_company  
from process_company_contributions import process_company_contributions
import logging


@task(
    name="update_companies_for_individuals",
    depends_on=["process_individual_contributions"],
    inputs=["individuals"],
    outputs=["companies"],
)
def update_companies_for_individuals(context, individual_ids=None):
    """
    Update company data when individuals are added or modified.
    
    This task ensures that:
    1. Companies' relatedIndividuals lists are updated
    2. Company contribution aggregations include new individual data
    3. Company party summaries are recalculated
    
    Args:
        individual_ids: List of individual IDs that were updated (optional)
    """
    if individual_ids:
        logging.info(f"Updating company data for individuals: {individual_ids}")
        
        # Find which companies are affected by these individuals
        affected_companies = set()
        for ind_id in individual_ids:
            if ind_id in context.db.individuals:
                individual = context.db.individuals[ind_id]
                if "company" in individual:
                    affected_companies.update(individual["company"])
        
        if affected_companies:
            logging.info(f"Affected companies: {list(affected_companies)}")
        else:
            logging.info("No companies affected by these individuals")
            return {"status": "no_companies_affected"}
    
    # Update company spending data (refreshes relatedIndividuals lists)
    logging.info("Updating company spending data...")
    update_spending_by_company(context.db, context.session)
    
    # Process company contributions (includes individual contributions)  
    logging.info("Processing company contributions with individual data...")
    new_recipients = process_company_contributions(context.db, context.session)
    
    return {
        "status": "success",
        "affected_companies": list(affected_companies) if individual_ids else "all",
        "new_recipients": list(new_recipients),
        "processed_individuals": individual_ids if individual_ids else "all"
    }


@task(
    name="complete_individual_workflow",
    depends_on=["fetch_individual_spending_selective"],
    outputs=["companies", "individuals"],
    run_by_default=False,
)
def complete_individual_workflow(context, individual_ids):
    """
    Complete workflow for adding/updating individuals including all dependencies.
    
    This is a convenience task that runs the full cascade:
    1. Processes individual contributions
    2. Updates affected company data
    3. Ensures all aggregations are consistent
    
    Args:
        individual_ids: List of individual IDs to process
    """
    logging.info(f"Running complete individual workflow for: {individual_ids}")
    
    # First process the individual contributions
    from tasks.individuals_selective import process_individual_contributions_selective
    ind_result = process_individual_contributions_selective(context, individual_ids)
    
    # Then update companies that are affected
    comp_result = update_companies_for_individuals(context, individual_ids)
    
    return {
        "status": "complete",
        "individual_result": ind_result,
        "company_result": comp_result,
        "processed_individuals": individual_ids
    }