def compute_company_state_spending(db):
    """Compute company spending by state based on recipient candidate associations.

    For each company contribution, looks up the recipient committee's associated
    candidate(s) to determine which state the money is going to. Skips contributions
    to super PACs and other broad committees without clear candidate associations.
    """
    # Load recipient data (maps committee_id -> candidate_details with state info)
    recipients_doc = db.client.collection("allRecipients").document("recipients").get()
    all_recipients = recipients_doc.to_dict() if recipients_doc.exists else {}

    # Load non-candidate committees to skip (same as get_beneficiaries)
    non_candidate_committees = db.non_candidate_committees or set()

    # Aggregate: { state: { company_id: total } }
    by_state = {}

    for doc in db.client.collection("companies").stream():
        company_id = doc.id
        company = doc.to_dict()
        contributions_list = company.get("contributions", [])

        for group in contributions_list:
            committee_id = group.get("committee_id")
            total = group.get("total", 0)
            if not committee_id or total <= 0:
                continue

            if committee_id in non_candidate_committees:
                continue

            recipient = all_recipients.get(committee_id)
            if not recipient:
                continue

            candidate_details = recipient.get("candidate_details", {})
            if not candidate_details:
                continue

            # Only count candidates from candidate_ids, not sponsor_candidate_ids
            candidate_ids = set(recipient.get("candidate_ids", []) or [])

            # Collect unique states from associated candidates
            states = set()
            for cid, candidate in candidate_details.items():
                if cid not in candidate_ids:
                    continue
                state = candidate.get("state")
                if state:
                    states.add(state)

            if not states:
                continue

            # Split evenly across states if multiple candidates in different states
            per_state_amount = round(total / len(states), 2)

            for state in states:
                if state not in by_state:
                    by_state[state] = {}
                if company_id not in by_state[state]:
                    by_state[state][company_id] = 0
                by_state[state][company_id] = round(
                    by_state[state][company_id] + per_state_amount, 2
                )

    # Load current expenditures.states and merge in by_companies
    states_doc = db.client.collection("expenditures").document("states").get()
    states_data = states_doc.to_dict() if states_doc.exists else {}

    for state in states_data:
        company_spending = by_state.get(state, {})
        states_data[state]["by_companies"] = company_spending
        states_data[state]["companies_total"] = round(
            sum(company_spending.values()), 2
        )

    # Also add states that have company spending but no expenditures
    for state in by_state:
        if state not in states_data:
            states_data[state] = {
                "total": 0,
                "by_committee": {},
                "by_race": {},
                "by_companies": by_state[state],
                "companies_total": round(sum(by_state[state].values()), 2),
            }

    db.client.collection("expenditures").document("states").set(states_data)
