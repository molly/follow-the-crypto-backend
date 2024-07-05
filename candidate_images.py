from Storage import Storage
from utils import get_first_last_name


to_skip = {
    "Peter Priest II",
    "Sean Dada",
    "Ralph Scott Jr.",
    "Timothy Ferreira",
    "Khalif Havens",
    "Anthony Marecki",
    "Michael Chandler",
    "Mark Hackett",
    "Clayton Chapman",
    "Dennis Aryan",
    "Michael Sigmon",
    "Susan Hall",
    "Chris Corey",
    "Aamir Arif",
    "Richard Siegel",
    "Lea Sherman",
    "Christian Robbins",
    "Alexander Dodenhoff",
    "Paul Briscoe",
}


def get_candidates_without_images(db):
    missing = []
    storage = Storage()
    race_docs = db.client.collection("raceDetails").stream()
    for doc in race_docs:
        state, state_data = doc.id, doc.to_dict()
        for race_id, race_data in state_data.items():
            for candidate in race_data["candidates"].values():
                if candidate["common_name"] in to_skip:
                    continue
                first_name, last_name = get_first_last_name(candidate["common_name"])
                filename = f"{first_name.lower()}-{last_name.lower()}.webp"
                if not storage.has_image(filename):
                    missing.append(candidate["common_name"])
    return missing
