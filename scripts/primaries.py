import csv
from datetime import date, timedelta


def primaries(db):
    candidates = (
        db.client.collection("candidates").document("bySpending").get().to_dict()
    )
    state_data = db.client.collection("expenditures").document("states").get().to_dict()
    expenditures = db.client.collection("expenditures").document("all").get().to_dict()
    primary_data = []
    for candidate in candidates["order"]:
        candidate_data = candidates["candidates"][candidate]
        primary_support = 0
        primary_oppose = 0
        state = candidate_data["state"]
        race_id = state + "-" + candidate_data["race"]

        race_expenditures = state_data[state]["by_race"][race_id]["expenditures"]
        for exp_id in race_expenditures:
            exp = expenditures[exp_id]
            if exp["candidate_id"] == candidate_data["candidate_id"] and (
                exp["subrace"] == "primary" or exp["subrace"] == "primary_runoff"
            ):
                if exp["support_oppose_indicator"] == "S":
                    primary_support += exp["expenditure_amount"]
                elif exp["support_oppose_indicator"] == "O":
                    primary_oppose += exp["expenditure_amount"]

        if primary_support > 0 or primary_oppose > 0:
            primary_happened = None
            race_details = (
                db.client.collection("raceDetails").document(state).get().to_dict()
            )
            race = race_details[candidate_data["race"]]
            for subrace in race["races"]:
                subrace_type = subrace.get("type", None)
                subrace_party = subrace.get("party", None)
                if (
                    subrace_type == "primary"
                    and subrace_party
                    and (
                        subrace_party == "N" or subrace_party == candidate_data["party"]
                    )
                ):
                    if subrace.get("canceled", False):
                        primary_happened = True
                        continue
                    race_date_str = subrace.get("date", None)
                    race_date = date.fromisoformat(subrace["date"])
                    if race_date > date.today():
                        primary_happened = False
                    else:
                        primary_happened = True
                    continue

            goal_achieved = None
            if primary_happened == False:
                goal_achieved = "UPCOMING"
            elif primary_support > 0 and primary_oppose > 0:
                goal_achieved = "INDETERMINATE"
            elif candidate_data.get("defeated", False) or candidate_data.get(
                "withdrew", False
            ):
                if primary_support > 0:
                    goal_achieved = False
                elif primary_oppose > 0:
                    goal_achieved = True
            else:
                if primary_support > 0:
                    goal_achieved = True
                elif primary_oppose > 0:
                    goal_achieved = False
            primary_data.append(
                [
                    candidate_data["common_name"],
                    candidate_data["state"],
                    candidate_data["race"],
                    candidate_data["party"],
                    round(primary_support, 2),
                    round(primary_oppose, 2),
                    candidate_data.get("defeated", False),
                    candidate_data.get("withdrew", False),
                    goal_achieved,
                ]
            )
    with open("primaries.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Candidate",
                "State",
                "Race",
                "Party",
                "Support spending",
                "Oppose spending",
                "Defeated?",
                "Withdrew?",
                "Spending goal achieved?",
            ]
        )
        for row in primary_data:
            writer.writerow(row)
