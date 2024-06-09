from google.cloud import storage


class Storage:
    _instance = None
    client = None
    bucket = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Storage, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if Storage.client is None:
            try:
                Storage.client = storage.Client(project="follow-the-crypto-33160")
            except Exception as error:
                print("Error connecting to storage: {}".format(error))
            else:
                print("Storage connected.")
        self.client = Storage.client
        self.bucket = self.client.get_bucket("candidates")

    def has_image(self, filename):
        return self.bucket.blob(filename).exists()

    def upload_image(self, filename, buffer):
        path = self.bucket.blob(filename)
        path.upload_from_file(buffer, content_type="image/webp", rewind=True)
        print("Uploaded " + filename)
