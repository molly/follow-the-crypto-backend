"""
Race merging utility for preserving manual race entries.

This module handles merging scraped race data with manually-entered race data
when saving to Firestore.
"""

from typing import List, Dict, Any, Optional
import logging
import time


def generate_race_key(race: Dict[str, Any]) -> str:
    """
    Generate a unique key for a race based on type, party, and date.

    Args:
        race: Race dictionary with 'type', 'party' (optional), and 'date'

    Returns:
        String key like "primary-R-2026-03-05"
    """
    race_type = race.get('type', '')
    party = race.get('party', 'none')
    date = race.get('date', '')
    return f"{race_type}-{party}-{date}"


def save_scraped_races(db_client, state: str, race_data: Dict[str, Any]) -> None:
    """
    Save scraped race data to Firestore in the scrapedRaces field.

    This function preserves existing manualRaces and races fields.
    The races field should only be updated via the manual review UI.

    Args:
        db_client: Firestore client instance
        state: State abbreviation (e.g., 'AZ', 'CA')
        race_data: Dictionary of race groups for the state
                   Format: {race_id: {"races": [...], "withdrew": {...}, ...}}

    Example:
        save_scraped_races(
            db_client=db.client,
            state='AZ',
            race_data={
                'H-01': {
                    'races': [
                        {'type': 'primary', 'party': 'R', 'date': '2026-03-05', ...}
                    ],
                    'withdrew': {...}
                }
            }
        )
    """
    doc_ref = db_client.collection('raceDetails').document(state)

    # Fetch existing data to preserve manualRaces and races fields
    doc = doc_ref.get()
    existing_data = doc.to_dict() if doc.exists else {}

    # Get current timestamp in milliseconds
    current_timestamp = int(time.time() * 1000)

    # Process each race group
    for race_id, race_group in race_data.items():
        scraped_races = race_group.get('races', [])

        # Get existing fields to preserve
        existing_group = existing_data.get(race_id, {})
        manual_races = existing_group.get('manualRaces', [])
        reviewed_races = existing_group.get('races', [])
        manual_races_updated = existing_group.get('manualRacesUpdated', 0)
        last_reviewed = existing_group.get('lastReviewed', 0)

        # Save scraped data to scrapedRaces field
        race_group['scrapedRaces'] = scraped_races
        race_group['scrapedRacesUpdated'] = current_timestamp

        # Preserve manualRaces field if it exists
        if manual_races:
            race_group['manualRaces'] = manual_races
            race_group['manualRacesUpdated'] = manual_races_updated
            logging.info(f"Preserved {len(manual_races)} manual races for {state}/{race_id}")

        # Preserve races field if it exists (this is the reviewed/merged data)
        if reviewed_races:
            race_group['races'] = reviewed_races
            race_group['lastReviewed'] = last_reviewed
            logging.info(f"Preserved {len(reviewed_races)} reviewed races for {state}/{race_id}")
        else:
            # If no reviewed races exist yet, initialize with scraped data
            race_group['races'] = scraped_races

        logging.info(
            f"{state}/{race_id}: Saved {len(scraped_races)} scraped races"
        )

    # Write back to Firestore
    doc_ref.set(race_data)
    logging.info(f"Saved scraped race details for {state} with {len(race_data)} race groups")
