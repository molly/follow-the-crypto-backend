IDS = {"C00849810"}


def find_committee_donor(db):
    for doc in db.client.collection("individuals").stream():
        str_id, individual = doc.id, doc.to_dict()
        for contrib_group in individual["contributions"]:
            if contrib_group["committee_id"] in IDS:
                print(f"{contrib_group['committee_id']}: {str_id}")

    for doc in db.client.collection("companies").stream():
        str_id, individual = doc.id, doc.to_dict()
        for contrib_group in individual["contributions"]:
            if contrib_group["committee_id"] in IDS:
                print(f"{contrib_group['committee_id']}: {str_id}")
