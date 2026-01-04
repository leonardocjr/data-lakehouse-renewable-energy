"""
Helpers for normalizing US state names and filtering pollutants.
"""

from typing import Optional

# Mapping of full state names to two-letter postal codes.
STATE_NAME_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    # Territories and common regions that may appear in public datasets.
    "puerto rico": "PR",
    "guam": "GU",
    "american samoa": "AS",
    "northern mariana islands": "MP",
    "u.s. virgin islands": "VI",
    "us virgin islands": "VI",
}

# Region aliases sometimes returned by EIA; they will not match state-level joins
# but are kept uppercase for traceability.
REGION_ALIASES = {
    "pacific": "PACIFIC",
    "mountain": "MOUNTAIN",
    "midwest": "MIDWEST",
    "central": "CENTRAL",
    "south": "SOUTH",
    "northeast": "NORTHEAST",
    "new england": "NEW_ENGLAND",
}


def normalize_state(raw: Optional[str]) -> Optional[str]:
    """
    Normalize a state description into a two-letter code when possible.
    Falls back to upper-cased input when the value is already a code or a region.
    """
    if raw is None:
        return None

    value = str(raw).strip()
    if not value:
        return None

    if len(value) == 2 and value.isalpha():
        return value.upper()

    key = value.lower()
    if key in STATE_NAME_TO_CODE:
        return STATE_NAME_TO_CODE[key]
    if key in REGION_ALIASES:
        return REGION_ALIASES[key]

    return value.upper()


def is_co2_pollutant(raw: Optional[str]) -> bool:
    """
    Determine whether a pollutant string refers to CO2 or equivalent greenhouse gases.
    """
    if raw is None:
        return False
    value = str(raw).lower()
    return "co2" in value or "carbon dioxide" in value or "co\u2082" in value
