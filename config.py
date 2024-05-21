import os

from dotenv import load_dotenv

load_dotenv()


class FECDataConfig:
    """Configuration for FEC data API operations"""
    api_key: str

    def __init__(self):
        self.api_key = os.getenv("FEC_API_KEY")


fec_data_config = FECDataConfig()
