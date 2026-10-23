from unidecode import unidecode

from states import SPECIAL_ELECTIONS, CURRENT_CYCLE
from utils import get_expenditure_race_type, get_sector_keys
from race_utils import get_all_races
from expenditure_utils import get_all_expenditures, set_all_expenditures


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


# raceDetails rosters record party as a single letter; expenditures use FEC codes.
ROSTER_PARTY_TO_FEC = {"D": "DEM", "R": "REP", "I": "IND", "L": "LIB", "G": "GRE"}


def build_roster_maps(db):
    """Four lookups over the raceDetails rosters, built in a single pass.

    `cid_to_seat` is candidate_id -> base race id (e.g. 'NY-H-15'). The FEC
    candidate_id is authoritative for which seat a candidate is running for,
    whereas the filer-entered candidate_office_district is free text and is
    sometimes mis-keyed. Only candidate_ids rostered in a single seat are
    included; an id that appears in more than one seat is ambiguous and left to
    the district field. Special-election variants ('NY-H-15-special') collapse
    to the same base seat, so they don't count as a second seat.

    `cid_to_name` is candidate_id -> roster common_name. The filer-entered name
    fields are unreliable in the same way the district field is: FEC files
    Ashley Hinson under her spouse's surname ("ARENHOLZ") with "ASHLEY HINSON"
    packed into candidate_first_name, so a name assembled from those fields
    reads "Ashley Arenholz" and matches no roster entry. An id whose rostered
    names disagree is left out rather than asserting one of them.

    `cid_to_party` is candidate_id -> FEC party code. Efiled 24/48-hour reports
    often leave candidate_party blank, which drops the row from the by-party
    totals. As with names, an id whose rostered parties disagree is left out.

    `race_by_id` is full race id ('IA-S') -> race data, for resolving the
    sub-race an expenditure belongs to from its date.
    """
    cid_to_seats = {}
    cid_to_names = {}
    cid_to_parties = {}
    race_by_id = {}
    for state, races in get_all_races(db.client).items():
        for race_id, race_data in races.items():
            race_by_id[f"{state}-{race_id}"] = race_data
            base = (
                race_id[: -len("-special")]
                if race_id.endswith("-special")
                else race_id
            )
            seat = f"{state}-{base}"
            for name, candidate in (race_data.get("candidates") or {}).items():
                cid = candidate.get("candidate_id")
                if not cid:
                    continue
                cid = db.candidate_aliases.get(cid, cid)
                cid_to_seats.setdefault(cid, set()).add(seat)
                cid_to_names.setdefault(cid, set()).add(
                    candidate.get("common_name") or name
                )
                party = candidate.get("party")
                # A handful of roster entries store party as a one-item list.
                if isinstance(party, list) and len(party) == 1:
                    party = party[0]
                if party in ROSTER_PARTY_TO_FEC:
                    cid_to_parties.setdefault(cid, set()).add(ROSTER_PARTY_TO_FEC[party])
    return (
        {cid: seats.pop() for cid, seats in cid_to_seats.items() if len(seats) == 1},
        {cid: names.pop() for cid, names in cid_to_names.items() if len(names) == 1},
        {cid: parties.pop() for cid, parties in cid_to_parties.items() if len(parties) == 1},
        race_by_id,
    )


def match_roster_id(expenditure, race, db):
    """The rostered candidate_id for an expenditure filed without one, or None.

    Efiled 24/48-hour reports sometimes omit candidate_id entirely (Abundant
    Future's CA-11 filings name only "Wiener" or "Chan"). Within the race the
    filing names, the id is taken from the one rostered candidate whose name
    contains every word of the filer's last name. Whole words, not substrings, so
    "LEE" can't match "Kathleen". No match or more than one leaves it unset.
    """
    last = unidecode(expenditure.get("candidate_last_name") or "").upper().split()
    if not last:
        return None
    matches = set()
    for name, candidate in (race.get("candidates") or {}).items():
        cid = candidate.get("candidate_id")
        if cid and set(last) <= set(unidecode(name).upper().split()):
            matches.add(db.candidate_aliases.get(cid, cid))
    return matches.pop() if len(matches) == 1 else None


def resolve_from_roster(db, expenditure, cid_to_seat, cid_to_name, cid_to_party, race_by_id):
    """Stamp roster-resolved candidate fields and sub-race onto an expenditure.

    Every consumer needs these and none can derive them reliably from the
    filer-entered fields, so they're settled once here rather than reassembled
    at each render site:

      - `candidate_id`: filled when the filing omitted it (see match_roster_id).
      - `candidate_party`: filled from the roster when the filing left it blank.
      - `candidate_common_name`: the name the rest of the site calls this
        candidate (see build_roster_maps).
      - `subrace`: efiled expenditures carry no election_type at all, so
        get_expenditure_race_type can only resolve them by comparing the
        expenditure's date against the race's sub-race dates -- which needs the
        roster passed in. Without this the row keeps a null subrace and matches
        no sub-race view, orphaning the spending.
    """
    race = race_by_id.get(get_race_name(expenditure, cid_to_seat))
    if race and not expenditure.get("candidate_id"):
        cid = match_roster_id(expenditure, race, db)
        if cid:
            expenditure["candidate_id"] = cid
            # The id may be rostered in a different seat than the district field
            # named, so re-resolve the race from it.
            race = race_by_id.get(get_race_name(expenditure, cid_to_seat))

    cid = expenditure.get("candidate_id")
    name = cid_to_name.get(cid)
    if name:
        expenditure["candidate_common_name"] = name
    if not expenditure.get("candidate_party") and cid in cid_to_party:
        expenditure["candidate_party"] = cid_to_party[cid]

    if race:
        # Returns an existing usable subrace untouched, and only falls through to
        # date matching for the null and "other" cases.
        subrace = get_expenditure_race_type(expenditure, race.get("races") or [])
        if subrace:
            expenditure["subrace"] = subrace


def get_race_name(expenditure, cid_to_seat=None):
    race = "{candidate_office_state}-{candidate_office}".format(**expenditure)
    if (
        expenditure["candidate_office_district"]
        and int(expenditure["candidate_office_district"]) != 0
    ):
        race += "-" + expenditure["candidate_office_district"]
    # The filer-entered candidate_office_district is unreliable (e.g. a Torres
    # NY-15 IE filed under district 25), which strands the spending under a
    # non-existent race. The candidate_id is authoritative, so when it's rostered
    # in exactly one seat, trust that seat over the district field.
    if cid_to_seat:
        seat = cid_to_seat.get(expenditure.get("candidate_id"))
        if seat and seat != race:
            race = seat
    entry = SPECIAL_ELECTIONS.get(race)
    if entry:
        election_type = expenditure.get("election_type") or ""
        # A current-cycle special-only seat has no regular contest, so every
        # expenditure there is the special regardless of how the filer coded
        # election_type. For seats that also hold a regular election (or whose
        # special is historical), trust the FEC election_type: "S..." means
        # "special". Gating on SPECIAL_ELECTIONS membership avoids creating
        # spurious special entries from misfiled election_types elsewhere.
        if entry["year"] == CURRENT_CYCLE and not entry["has_regular"]:
            race += "-special"
        elif election_type.startswith("S"):
            race += "-special"
    return race


def process_expenditures(db):
    all_expenditures = get_all_expenditures(db)
    cid_to_seat, cid_to_name, cid_to_party, race_by_id = build_roster_maps(db)

    # Resolve ids, parties, names and sub-races across every row before anything reads them.
    # Deliberately not folded into the aggregation loop below, which skips
    # zero-amount rows -- those still get rendered in the recent-expenditure
    # lists, so they need resolving too.
    for expenditure in all_expenditures.values():
        resolve_from_roster(
            db, expenditure, cid_to_seat, cid_to_name, cid_to_party, race_by_id
        )
    set_all_expenditures(db, all_expenditures)

    states = {}
    new_opposition_spending = set()

    def empty_parties():
        return {
            "dem_oppose": 0,
            "dem_support": 0,
            "rep_oppose": 0,
            "rep_support": 0,
            "oppose_benefit_dem": 0,
            "oppose_benefit_rep": 0,
            "oppose_benefit_mix": 0,
            "oppose_benefit_unk": 0,
        }

    all_parties = {
        "all": empty_parties(),
        "crypto": empty_parties(),
        "ai": empty_parties(),
    }
    committees = {}

    def empty_expenditure_totals():
        return {"total": 0, "by_committee": {}}

    totals = {
        "all": empty_expenditure_totals(),
        "crypto": empty_expenditure_totals(),
        "ai": empty_expenditure_totals(),
    }
    for uid, expenditure in all_expenditures.items():
        if not expenditure["expenditure_amount"]:
            continue
        race = get_race_name(expenditure, cid_to_seat)
        committee_id = expenditure["committee_id"]
        state = expenditure["candidate_office_state"]
        if state is None:
            state = "US"

        committee_sector = db.committees.get(committee_id, {}).get("sector")
        sector_keys = get_sector_keys(committee_sector)

        for key in sector_keys:
            totals[key]["total"] += expenditure["expenditure_amount"]
            if committee_id not in totals[key]["by_committee"]:
                totals[key]["by_committee"][committee_id] = expenditure["expenditure_amount"]
            else:
                totals[key]["by_committee"][committee_id] += expenditure["expenditure_amount"]

        # Initialize state and set total
        if state not in states:
            states[state] = {
                "total": expenditure["expenditure_amount"],
                "by_committee": {},
                "by_race": {},
            }
        else:
            states[state]["total"] = round(
                states[state]["total"] + expenditure["expenditure_amount"], 2
            )

        # Initialize committee and record
        if committee_id not in states[state]["by_committee"]:
            states[state]["by_committee"][committee_id] = {
                "total": expenditure["expenditure_amount"],
                "expenditures": [uid],
            }
        else:
            states[state]["by_committee"][committee_id]["total"] = round(
                states[state]["by_committee"][committee_id]["total"]
                + expenditure["expenditure_amount"],
                2,
            )
            states[state]["by_committee"][committee_id]["expenditures"].append(uid)

        # Initialize race and record
        if race not in states[state]["by_race"]:
            states[state]["by_race"][race] = {
                "total": expenditure["expenditure_amount"],
                "details": {
                    "candidate_office": expenditure["candidate_office"],
                    "candidate_office_district": expenditure[
                        "candidate_office_district"
                    ],
                },
                "expenditures": [uid],
            }
        else:
            states[state]["by_race"][race]["total"] = round(
                states[state]["by_race"][race]["total"]
                + expenditure["expenditure_amount"],
                2,
            )
            states[state]["by_race"][race]["expenditures"].append(uid)

        # Record party support/oppose for all committees, and per-committee
        if committee_id not in committees:
            committees[committee_id] = {
                "dem_support": 0,
                "dem_oppose": 0,
                "rep_support": 0,
                "rep_oppose": 0,
                "oppose_benefit_dem": 0,
                "oppose_benefit_rep": 0,
                "oppose_benefit_mix": 0,
                "oppose_benefit_unk": 0,
            }
        if expenditure["support_oppose_indicator"] == "S":
            if expenditure["candidate_party"] == "DEM":
                committees[committee_id]["dem_support"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["dem_support"] += expenditure["expenditure_amount"]
            elif expenditure["candidate_party"] == "REP":
                committees[committee_id]["rep_support"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["rep_support"] += expenditure["expenditure_amount"]
        elif expenditure["support_oppose_indicator"] == "O":
            if expenditure["candidate_party"] == "DEM":
                committees[committee_id]["dem_oppose"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["dem_oppose"] += expenditure["expenditure_amount"]
            elif expenditure["candidate_party"] == "REP":
                committees[committee_id]["rep_oppose"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["rep_oppose"] += expenditure["expenditure_amount"]
            if expenditure["candidate_id"] in db.opposition_spending:
                party = db.opposition_spending[expenditure["candidate_id"]][
                    "benefitsParty"
                ]
                if party == "DEM":
                    committees[committee_id]["oppose_benefit_dem"] += expenditure[
                        "expenditure_amount"
                    ]
                    for key in sector_keys:
                        all_parties[key]["oppose_benefit_dem"] += expenditure[
                            "expenditure_amount"
                        ]
                elif party == "REP":
                    committees[committee_id]["oppose_benefit_rep"] += expenditure[
                        "expenditure_amount"
                    ]
                    for key in sector_keys:
                        all_parties[key]["oppose_benefit_rep"] += expenditure[
                            "expenditure_amount"
                        ]
                elif party == "MIX":
                    committees[committee_id]["oppose_benefit_mix"] += expenditure[
                        "expenditure_amount"
                    ]
                    for key in sector_keys:
                        all_parties[key]["oppose_benefit_mix"] += expenditure[
                            "expenditure_amount"
                        ]
            else:
                committees[committee_id]["oppose_benefit_unk"] += expenditure[
                    "expenditure_amount"
                ]
                for key in sector_keys:
                    all_parties[key]["oppose_benefit_unk"] += expenditure[
                        "expenditure_amount"
                    ]
                new_opposition_spending.add(expenditure["candidate_id"])

    db.client.collection("expenditures").document("states").set(states)
    for committee_id, committee_data in committees.items():
        db.client.collection("committees").document(committee_id).set(
            {"by_party": committee_data}, merge=True
        )
    db.client.collection("expenditures").document("total").set(totals)

    # Get most recent for committee, all, and by sector
    most_recent_all = [x["uid"] for x in sort_and_slice(all_expenditures.values(), 50)]
    most_recent_by_committee = {}

    committee_ids = [committee["id"] for committee in db.committees.values()]
    for committee_id in committee_ids:
        most_recent_by_committee[committee_id] = [
            x["uid"]
            for x in sort_and_slice(
                filter(
                    lambda x: x["committee_id"] == committee_id,
                    all_expenditures.values(),
                )
            )
        ]

    crypto_committee_ids_set = {
        cid for cid, c in db.committees.items()
        if c.get("sector") in ("crypto", "tech")
    }
    ai_committee_ids_set = {
        cid for cid, c in db.committees.items()
        if c.get("sector") in ("ai", "tech")
    }
    most_recent_crypto = [
        x["uid"]
        for x in sort_and_slice(
            [
                e
                for e in all_expenditures.values()
                if e["committee_id"] in crypto_committee_ids_set
            ],
            50,
        )
    ]
    most_recent_ai = [
        x["uid"]
        for x in sort_and_slice(
            [
                e
                for e in all_expenditures.values()
                if e["committee_id"] in ai_committee_ids_set
            ],
            50,
        )
    ]

    db.client.collection("expenditures").document("recent").set(
        {
            "all": {
                "all": most_recent_all,
                "by_committee": most_recent_by_committee,
            },
            "crypto": {
                "all": most_recent_crypto,
                "by_committee": {
                    k: v
                    for k, v in most_recent_by_committee.items()
                    if k in crypto_committee_ids_set
                },
            },
            "ai": {
                "all": most_recent_ai,
                "by_committee": {
                    k: v
                    for k, v in most_recent_by_committee.items()
                    if k in ai_committee_ids_set
                },
            },
        }
    )
    db.client.collection("expenditures").document("by_party").set(all_parties)
    return new_opposition_spending
