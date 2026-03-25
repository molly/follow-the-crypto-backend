from collections import defaultdict
from recipient_utils import get_all_recipients


def compute_company_state_spending(db):
    """Compute company spending by state based on recipient candidate associations.

    For each company contribution, looks up the recipient committee's associated
    candidate(s) to determine which state the money is going to. Skips contributions
    to super PACs and other broad committees without clear candidate associations.

    Counts contributions to all candidates (including non-2026) in the main total.
    Separately tracks contributions to states where no 2026 candidates are present,
    stored as prior_cycle_details and prior_cycle_companies_total.
    """
    # Load recipient data (maps committee_id -> candidate_details with state info)
    all_recipients = get_all_recipients(db)

    # Load non-candidate committees to skip (same as get_beneficiaries)
    non_candidate_committees = db.non_candidate_committees or set()

    # Aggregate: { state: { company_id: total } }
    by_state = {}

    # Prior cycle contributions: { state: [{ company_id, company_name, committee_id, committee_name, amount, candidates }] }
    prior_cycle_by_state = defaultdict(list)

    for doc in db.client.collection("companies").stream():
        company_id = doc.id
        company = doc.to_dict()
        company_name = db.companies.get(company_id, {}).get("name", company_id)
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

            # Collect all states, and separately track which have 2026 candidates
            all_candidate_states = set()
            running_states = set()
            # Track excluded candidates per state for prior cycle reporting
            excluded_candidates_by_state = defaultdict(list)

            for cid, candidate in candidate_details.items():
                if cid not in candidate_ids:
                    continue
                state = candidate.get("state")
                if not state:
                    continue
                all_candidate_states.add(state)
                if candidate.get("isRunningThisCycle", False):
                    running_states.add(state)
                else:
                    excluded_candidates_by_state[state].append({
                        "name": candidate.get("name", ""),
                        "office": candidate.get("office", ""),
                        "election_years": candidate.get("election_years", []),
                    })

            if not all_candidate_states:
                continue

            # Split evenly across all states (including prior cycle)
            per_state_amount = round(total / len(all_candidate_states), 2)

            for state in all_candidate_states:
                if state not in by_state:
                    by_state[state] = {}
                if company_id not in by_state[state]:
                    by_state[state][company_id] = 0
                by_state[state][company_id] = round(
                    by_state[state][company_id] + per_state_amount, 2
                )

            # Track prior cycle: states with no 2026 candidates
            dropped_states = all_candidate_states - running_states
            committee_name = recipient.get("committee_name", "")
            for state in dropped_states:
                prior_cycle_by_state[state].append({
                    "company_id": company_id,
                    "company_name": company_name,
                    "committee_id": committee_id,
                    "committee_name": committee_name,
                    "amount": per_state_amount,
                    "candidates": excluded_candidates_by_state.get(state, []),
                })

    # Load current expenditures.states and merge in by_companies
    states_doc = db.client.collection("expenditures").document("states").get()
    states_data = states_doc.to_dict() if states_doc.exists else {}

    for state in states_data:
        company_spending = by_state.get(state, {})
        prior_cycle = prior_cycle_by_state.get(state, [])
        states_data[state]["by_companies"] = company_spending
        states_data[state]["companies_total"] = round(
            sum(company_spending.values()), 2
        )
        states_data[state]["prior_cycle_details"] = prior_cycle
        states_data[state]["prior_cycle_companies_total"] = round(
            sum(e["amount"] for e in prior_cycle), 2
        )

    # Also add states that have company spending but no expenditures
    for state in by_state:
        if state not in states_data:
            prior_cycle = prior_cycle_by_state.get(state, [])
            states_data[state] = {
                "total": 0,
                "by_committee": {},
                "by_race": {},
                "by_companies": by_state[state],
                "companies_total": round(sum(by_state[state].values()), 2),
                "prior_cycle_details": prior_cycle,
                "prior_cycle_companies_total": round(
                    sum(e["amount"] for e in prior_cycle), 2
                ),
            }

    db.client.collection("expenditures").document("states").set(states_data)
