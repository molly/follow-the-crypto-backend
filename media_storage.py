"""Object storage for rehosted ad media (videos and their poster frames).

Media goes in the existing `follow-the-crypto-ads` bucket, alongside the image
ads already stored there. `Storage.py` can't be reused as-is: it's a singleton
pinned to the `candidates` bucket with an upload method hardcoded to webp.

What gets stored in Firestore is the object *key*, not a full URL. The frontend
prepends a base (see MEDIA_BASE_URL in the app), so moving the bucket behind a
CDN later is a one-line frontend change instead of a rewrite of every stored ad
record.
"""

from google.cloud import storage

BUCKET_NAME = "follow-the-crypto-ads"

# Keys are derived from immutable ad ids, so an object at a given key never
# changes. Let the browser and any CDN in front of the bucket cache it forever.
IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable"


class MediaStorage:
    _instance = None
    client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MediaStorage, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if MediaStorage.client is None:
            MediaStorage.client = storage.Client(project="follow-the-crypto-33160")
            print("Media storage connected.")
        self.client = MediaStorage.client
        self.bucket = self.client.bucket(BUCKET_NAME)

    def exists(self, key):
        return self.bucket.blob(key).exists()

    def upload(self, key, path, content_type):
        """Upload a local file and return the key it was stored under."""
        blob = self.bucket.blob(key)
        blob.cache_control = IMMUTABLE_CACHE_CONTROL
        blob.upload_from_filename(path, content_type=content_type)
        print("Uploaded " + key)
        return key
