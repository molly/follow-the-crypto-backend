import firebase_admin
from firebase_admin import credentials, firestore
import re


class Database:
    def __init__(self):
        cred = credentials.Certificate("service.json")
        firebase_admin.initialize_app(cred)
        self.client = firestore.client()
        self.committees = None
        self.company_aliases = None
        self.individual_employers = None
        self.occupation_allowlist = None
        self.duplicate_contributions = None

    def get_constants(self):
        constants = self.client.collection("constants")
        self.committees = constants.document("committees").get().to_dict()
        self.company_aliases = constants.document("companyAliases").get().to_dict()
        individual_employers_dict = (
            constants.document("individualEmployers").get().to_dict()
        )
        self.individual_employers = set(
            individual_employers_dict["individualEmployers"]
        )
        self.occupation_allowlist = (
            constants.document("occupationAllowlist").get().to_dict()
        )
        self.occupation_allowlist["contains"] = re.compile(
            "({})".format("|".join(self.occupation_allowlist["contains"])),
            re.IGNORECASE,
        )
        self.occupation_allowlist["equals"] = set(self.occupation_allowlist["equals"])
        self.duplicate_contributions = (
            constants.document("duplicateContributions").get().to_dict()
        )
