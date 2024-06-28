import datetime


def sort_and_slice(lst, length=10):
    def get_date(x):
        date = x.get("expenditure_date")
        if not date:
            date = x.get("dissemination_date")
        return date if date else "0"

    return sorted(
        lst,
        key=get_date,
        reverse=True,
    )[:length]


def process_recent_expenditures(db):
    all_expenditures = []
    expenditures_by_committee = {}
    expenditures_by_state = {}

    recorded_state_expenditures = db.client.collection("expendituresByState").stream()
    for doc in recorded_state_expenditures:
        state, state_data = doc.id, doc.to_dict()
        for committee_id in state_data["by_committee"].keys():
            for expenditure in state_data["by_committee"][committee_id]["expenditures"]:
                all_expenditures.append(expenditure)

                if committee_id not in expenditures_by_committee:
                    expenditures_by_committee[committee_id] = []
                expenditures_by_committee[committee_id].append(expenditure)

                if state not in expenditures_by_state:
                    expenditures_by_state[state] = []
                expenditures_by_state[state].append(expenditure)

    db.client.collection("expenditures").document("all").set(
        {
            "recent": sort_and_slice(all_expenditures, 25),
        },
        merge=True,
    )

    for committee_id, expenditures in expenditures_by_committee.items():
        db.client.collection("expenditures").document("committee").set(
            {
                committee_id: {"recent": sort_and_slice(expenditures)},
            },
            merge=True,
        )

    for state, expenditures in expenditures_by_state.items():
        db.client.collection("expendituresByState").document(state).set(
            {
                "recent": sort_and_slice(expenditures),
            },
            merge=True,
        )
