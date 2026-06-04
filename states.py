STATES_BY_ABBR = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "US": "United States",
    "UT": "Utah",
    "VI": "United States Virgin Islands",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

STATES_BY_FULL = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "District of Columbia": "DC",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "United States": "US",
    "Utah": "UT",
    "United States Virgin Islands": "VI",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
}

SINGLE_MEMBER_STATES = ["AK", "DE", "ND", "SD", "VT", "VI", "WY"]

# Overrides for state names as they appear in Ballotpedia page titles,
# for states where the Ballotpedia name differs from the standard full name.
BALLOTPEDIA_STATE_NAMES = {
    "VI": "the Virgin Islands",
}

# The election cycle currently being tracked. Bump this once per cycle: the
# SPECIAL_ELECTIONS entries and all special-vs-regular routing key off it, so
# annual rollover is a one-line change rather than scattered literals.
CURRENT_CYCLE = 2026

# Known special elections.
#
# Hand-maintained domain knowledge: which seats hold a special election, in what
# year, and whether a REGULAR election for the same seat also happens in the same
# cycle. This cannot be derived reliably from FEC data — filers code
# election_type inconsistently and direct (company) contributions carry no
# election field at all — so it must be kept current by hand.
#
# Each entry maps a base seat id ("{state}-{office}[-district]") to:
#   "year"        - the special election year; drives the Ballotpedia URL.
#   "has_regular" - True if a regular election for the same seat ALSO occurs in
#                   the same cycle, so the seat needs both a "<seat>" (regular)
#                   and a "<seat>-special" race. False if the special is the only
#                   contest for the seat this cycle.
#
# Maintenance / drift:
#   - Add a seat when a new special is called; bump or retire "year" once the
#     special is over and no longer needs election_type-"S" routing.
#   - A special whose year != CURRENT_CYCLE is treated as historical: direct
#     contributions (which can't be split by election) route only to the regular
#     race, while election_type-"S" PAC spending still routes to "-special".
#   - validate_special_elections() (race_utils) flags entries whose actual
#     spending buckets contradict their classification, so drift fails loudly.
SPECIAL_ELECTIONS = {
    "OH-S": {"year": 2026, "has_regular": False},  # special only
    "FL-S": {"year": 2026, "has_regular": False},  # special only
    "NJ-H-11": {"year": 2026, "has_regular": True},  # special + regular
    "GA-H-14": {"year": 2026, "has_regular": True},  # special + regular
    "CA-H-01": {"year": 2026, "has_regular": True},  # special + regular
    # Specials that completed in 2025; the regular 2026 race is the live contest.
    # Kept here so election_type-"S" PAC spending still routes to "-special".
    "FL-H-06": {"year": 2025, "has_regular": True},
    "VA-H-11": {"year": 2025, "has_regular": True},
    "FL-H-01": {"year": 2025, "has_regular": True},
}


def special_election_year(seat):
    """Return the special-election year for a base seat id, or None."""
    entry = SPECIAL_ELECTIONS.get(seat)
    return entry["year"] if entry else None


def is_current_special(seat):
    """True if the seat holds a special election in the current cycle."""
    entry = SPECIAL_ELECTIONS.get(seat)
    return bool(entry) and entry["year"] == CURRENT_CYCLE


def canonical_race_keys(seat):
    """Canonical race key(s) for a base seat id (e.g. "OH-S", "GA-H-14").

    Shared by the company-spending pipeline and the race-detail scraper so they
    can't drift on how a seat's special/regular contests are keyed:
      - current-cycle special-only seat -> ["<seat>-special"]
      - current-cycle special + regular -> ["<seat>", "<seat>-special"]
      - regular seat, or a past special  -> ["<seat>"]

    This is for sources that can't self-identify the election (direct company
    contributions). PAC expenditures carry election_type and are keyed by
    process_committee_expenditures.get_race_name instead.
    """
    if not is_current_special(seat):
        return [seat]
    if SPECIAL_ELECTIONS[seat]["has_regular"]:
        return [seat, f"{seat}-special"]
    return [f"{seat}-special"]
