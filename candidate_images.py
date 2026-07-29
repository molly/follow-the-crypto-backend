from Storage import Storage
from utils import get_first_last_name
from race_utils import get_all_races


to_skip = {
    "John Brendan Williams",
    "Houston Brignano",
    "Rustin Knudtson",
    "Ha Phan",
    "Nicholas Finan",
    "Stephanie Vargas",
    "Hannah James",
    "Parminder Singh",
    "Amanda Capobianco",
    "Carl Lambrecht",
    "Steve Chasse",
    "Eugene Douglass",
    "Christopher Manuel Alcantara",
    "Tevin Channing Minus",
    "Angela Marie Walls-Windhauser",
    "Chris Henry",
    "John Minarcik",
    "Thomas Rudd",
    "David Hatfield",
    "John Field",
    "Edward Dunn",
    "Chandiha Gajapathy",
    "Gavin Solomon",
    "Robb Huhn",
    "Kaley Aldrich",
    "Charles Hoelter",
    "Leah Toomim",
    "Thomas Vo",
    "Blake Stanley",
    "Samuel Forstag",
    "Jullie Kelley",
    "Joshua Kolasinski",
    "Kirt Burgess",
    "Jeffrey Kanter",
    "Carl Harris Sr.",
    "Jean Depalis",
    "Deborah Calhoun Rhodes",
    "Tisha Benoit",
    "Andromeda Crum",
    "David Anderson",
    "John Dority",
    "Matthew Sin",
    "Lewis Mizrahi",
    "Jared Kane",
    "Nathan Tracy",
    "Wilfred Curioso"
}


def get_candidates_without_images(db):
    missing = []
    storage = Storage()
    for state, state_data in get_all_races(db.client).items():
        for race_id, race_data in state_data.items():
            for candidate in race_data["candidates"].values():
                if candidate["common_name"] in to_skip:
                    continue
                first_name, last_name = get_first_last_name(candidate["common_name"])
                filename = f"{first_name.lower()}-{last_name.lower()}.webp"
                if not storage.has_image(filename):
                    missing.append(candidate["common_name"])
    return missing
