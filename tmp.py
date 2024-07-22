IDS = {"C00846303", "C00863373", "C00693895", "C00865220"}


def tmp(db):
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
