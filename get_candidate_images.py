import google.cloud.logging
from Database import Database
from Storage import Storage
from utils import get_first_last_name
from google_images_search import GoogleImagesSearch
from msecrets import GOOGLE_IMAGES_API_KEY, GOOGLE_IMAGES_CX
import cv2
import matplotlib.pyplot as plt
from io import BytesIO
from PIL import Image
import numpy
from states import STATES_BY_ABBR
import requests

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
}


def double_square_size(x, y, w, h, image_width, image_height):
    side_length = w
    center_x = x + side_length / 2
    center_y = y + side_length / 2

    # Double the side length
    new_side_length = side_length * 2

    # Calculate the new top-left coordinates to maintain the centroid
    new_x = center_x - new_side_length / 2
    new_y = center_y - new_side_length / 2

    # Adjust the side length to ensure the square fits within the image boundaries
    new_side_length = min(new_side_length, image_width, image_height)

    # Adjust the new top-left coordinates to ensure the square fits within the image
    new_x = max(0, min(new_x, image_width - new_side_length))
    new_y = max(0, min(new_y, image_height - new_side_length))

    # Convert the coordinates and side length to integers
    new_x = int(new_x)
    new_y = int(new_y)
    new_side_length = int(new_side_length)

    # Calculate the coordinates of left, top, right, bottom
    left = new_x
    top = new_y
    right = new_x + new_side_length
    bottom = new_y + new_side_length

    return left, top, right, bottom


def resize_to_square_within_image(x, y, w, h, image_width, image_height):
    center_x = x + w / 2
    center_y = y + h / 2

    # Determine the new side length of the square
    side_length = max(w, h)

    # Calculate new top-left coordinates to maintain the centroid
    new_x = center_x - side_length / 2
    new_y = center_y - side_length / 2

    # Check if the new square exceeds image boundaries and adjust if necessary
    if new_x < 0:
        new_x = 0
    if new_y < 0:
        new_y = 0
    if new_x + side_length > image_width:
        new_x = image_width - side_length
    if new_y + side_length > image_height:
        new_y = image_height - side_length

    # Adjust new top-left coordinates and side length to ensure they fit within the image
    new_x = max(0, min(new_x, image_width - side_length))
    new_y = max(0, min(new_y, image_height - side_length))

    # Adjust side length to fit within image bounds if necessary
    new_side_length = min(side_length, image_width - new_x, image_height - new_y)

    new_x = int(new_x)
    new_y = int(new_y)
    new_side_length = int(new_side_length)

    return new_x, new_y, new_side_length, new_side_length


def process_image(candidate, img_data, storage):
    img = cv2.cvtColor(numpy.array(img_data), cv2.COLOR_RGB2BGR)
    grayscale = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    face_classifier = cv2.CascadeClassifier(
        cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
    )
    width = img_data.width
    height = img_data.height
    face = face_classifier.detectMultiScale(
        grayscale,
        scaleFactor=1.1,
        minNeighbors=5,
        minSize=(int(width / 10), int(height / 10)),
    )
    x, y, w, h = face[0]
    x, y, w, h = resize_to_square_within_image(x, y, w, h, width, height)
    cv2.rectangle(img, (x, y), (x + w, y + h), (0, 255, 0), 4)
    plt.figure(figsize=(20, 10))
    plt.imshow(img)
    plt.axis("off")
    plt.show()
    ok = input("Face OK? ")
    if ok == "y":
        resized_dimensions = double_square_size(x, y, w, h, width, height)
        cropped = img_data.crop(resized_dimensions)
        cropped.show()
        ok = input("Crop OK? ")
        if ok == "y":
            width = resized_dimensions[2] - resized_dimensions[0]
            if width > 500:
                # Don't need giant images
                cropped.thumbnail((500, 500), Image.LANCZOS)
            buffer = BytesIO()
            cropped.save(buffer, format="WEBP")
            filename = f"{candidate['filename']}.webp"
            storage.upload_image(filename, buffer)
        cropped.close()


def get_candidate_image(candidate, gis, storage):
    _search_params = {
        "q": f""""{candidate["common_name"]}" {STATES_BY_ABBR[candidate["state"]]}""",
        "num": 10,
    }
    print("Searching " + _search_params["q"])
    gis.search(search_params=_search_params)
    path = None
    bytes_io = BytesIO()
    ok = ""
    for image in gis.results():
        bytes_io.seek(0)
        raw_image_data = image.get_raw_data()
        image.copy_to(bytes_io, raw_image_data)
        bytes_io.seek(0)
        with Image.open(bytes_io) as tmp_img:
            tmp_img.show()
            ok = input("OK? ")
            if ok == "y":
                process_image(candidate, tmp_img, storage)
                break
            elif ok == "url":
                url = input("URL: ")
                resp = requests.get(url)
                with Image.open(BytesIO(resp.content)) as img:
                    process_image(candidate, img, storage)
                break
            elif ok == "skip":
                break


def get_candidate_images(db):
    storage = Storage()
    gis = GoogleImagesSearch(GOOGLE_IMAGES_API_KEY, GOOGLE_IMAGES_CX)
    candidates = []
    race_docs = db.client.collection("raceDetails").stream()
    for doc in race_docs:
        state, state_data = doc.id, doc.to_dict()
        for race_id, race_data in state_data.items():
            for candidate in race_data["candidates"].values():
                first_name, last_name = get_first_last_name(candidate["common_name"])
                candidates.append(
                    {
                        "common_name": candidate["common_name"],
                        "FEC_name": (
                            candidate["FEC_name"] if "FEC_name" in candidate else ""
                        ),
                        "party": (
                            candidate["party"][0]
                            if "party" in candidate and len(candidate["party"]) > 0
                            else ""
                        ),
                        "state": state,
                        "race": race_id,
                        "filename": f"{first_name.lower()}-{last_name.lower()}",
                    }
                )
    for candidate in candidates:
        if candidate["common_name"] in to_skip:
            print("Skipping: " + candidate["common_name"])
        elif storage.has_image(f"{candidate['filename']}.webp"):
            print("Image exists: " + candidate["filename"])
        else:
            get_candidate_image(candidate, gis, storage)


if __name__ == "__main__":
    client = google.cloud.logging.Client()
    client.setup_logging()

    db = Database()
    get_candidate_images(db)
