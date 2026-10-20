"""
Electioneering communications (FEC Form 9), converted to the same shape as the
Schedule E independent expenditures so the rest of the pipeline can treat them
as one set of spending.

Electioneering communications are broadcast ads that name a federal candidate
close to an election without expressly advocating for or against them. They're
filed by groups (often 501(c)(4)s) that never file Schedule E, and the FEC
excludes anything that qualifies as an independent expenditure from the
definition, so the two sets don't overlap.

Two things the Schedule E rows carry are missing here:
  - support_oppose_indicator: ECs have none, by definition. It's assigned from
    EC_SUPPORT_OPPOSE_BY_PARTY, a per-committee rule set by hand. Rows the rule
    doesn't cover are left out (and logged) rather than stored without one,
    since every consumer treats a missing indicator as opposition.
  - candidate_party: looked up from the FEC candidate record.

The processed /electioneering/ endpoint already drops rows from filings that
have since been amended. (Schedule B also lists Form 9 disbursement lines, but
keeps the superseded filings and carries no candidates, so it isn't used.)
"""

import logging

from utils import FEC_fetch, chunk, get_expenditure_race_type

# committee_id -> {candidate party: support_oppose_indicator}
EC_SUPPORT_OPPOSE_BY_PARTY = {
    # Alliance for a Better Future: GOP-focused; its ads support the Republicans
    # they name.
    "C30003628": {"REP": "S"},
}


def _get_candidate_parties(session, candidate_ids):
    parties = {}
    for ids_chunk in chunk(sorted(candidate_ids), 10):
        data = FEC_fetch(
            session,
            "electioneering candidates",
            "https://api.open.fec.gov/v1/candidates/",
            params={"candidate_id": ids_chunk, "per_page": 100},
        )
        if not data:
            raise RuntimeError(f"No FEC candidate data for {ids_chunk}")
        for candidate in data["results"]:
            parties[candidate["candidate_id"]] = candidate["party"]
    return parties


def _split_name(candidate_name):
    # FEC formats these as "LAST, FIRST MIDDLE".
    last, _, first = (candidate_name or "").partition(",")
    return last.strip(), first.strip()


def fetch_electioneering_communications(db, session, committee_ids):
    """Return tracked committees' electioneering communications, keyed by uid,
    in the Schedule E expenditure shape, plus the rows skipped for lack of a
    support/oppose rule (reported by the healthcheck)."""
    rows = []
    for ids_chunk in chunk(committee_ids, 10):
        page = 1
        while True:
            data = FEC_fetch(
                session,
                "electioneering communications",
                "https://api.open.fec.gov/v1/electioneering/",
                params={
                    "committee_id": ids_chunk,
                    "per_page": 100,
                    "page": page,
                },
            )
            if not data:
                raise RuntimeError(
                    f"No FEC electioneering data for {ids_chunk}, page {page}"
                )
            rows.extend(data["results"])
            if page >= data["pagination"]["pages"]:
                break
            page += 1

    for ec in rows:
        if ec.get("candidate_id") in db.candidate_aliases:
            ec["candidate_id"] = db.candidate_aliases[ec["candidate_id"]]

    parties = _get_candidate_parties(
        session, {ec["candidate_id"] for ec in rows if ec.get("candidate_id")}
    )

    transactions = {}
    skipped = []
    for ec in rows:
        committee_id = ec["committee_id"]
        party = parties.get(ec.get("candidate_id"))
        indicator = EC_SUPPORT_OPPOSE_BY_PARTY.get(committee_id, {}).get(party)
        if indicator is None:
            skipped.append(
                {
                    "committee_id": committee_id,
                    "candidate_id": ec.get("candidate_id"),
                    "candidate_name": ec.get("candidate_name"),
                    "party": party,
                    "amount": ec["calculated_candidate_share"],
                }
            )
            logging.warning(
                "Skipping electioneering communication %s from %s naming %s (%s): "
                "no support/oppose rule in EC_SUPPORT_OPPOSE_BY_PARTY",
                ec["sub_id"],
                committee_id,
                ec.get("candidate_name"),
                party,
            )
            continue

        last_name, first_name = _split_name(ec.get("candidate_name"))
        transaction_id = "EC-{}".format(ec["sub_id"])
        uid = "{}-{}".format(committee_id, transaction_id)
        transactions[uid] = {
            # The full disbursement repeats on each candidate's row when one ad
            # names several; the share is the per-candidate portion.
            "expenditure_amount": ec["calculated_candidate_share"],
            "expenditure_date": ec["disbursement_date"],
            "dissemination_date": ec["public_distribution_date"],
            "expenditure_description": ec["purpose_description"],
            "payee_name": ec["payee_name"],
            "candidate_id": ec.get("candidate_id"),
            "candidate_name": ec.get("candidate_name"),
            "candidate_first_name": first_name,
            "candidate_last_name": last_name,
            "candidate_middle_name": None,
            "candidate_suffix": None,
            "candidate_office": ec["candidate_office"],
            "candidate_office_state": ec["candidate_state"],
            "candidate_office_district": ec["candidate_district"],
            "candidate_party": party,
            "category_code": None,
            "category_code_full": None,
            "election_type": ec["election_type"],
            "support_oppose_indicator": indicator,
            "transaction_id": transaction_id,
            "subrace": None,
            "committee_id": committee_id,
            "uid": uid,
            "source": "electioneering",
        }
        transactions[uid]["subrace"] = get_expenditure_race_type(transactions[uid])
    return transactions, skipped
