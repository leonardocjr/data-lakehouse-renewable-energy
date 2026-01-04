"""
Data normalization and aggregation helpers for EIA and EPA datasets.
"""

from __future__ import annotations

import math
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

# Airflow plugin loader imports files as top-level modules, so relative imports
# can fail. Make sure the plugins root is on sys.path, then try multiple import styles.
PLUGINS_ROOT = Path(__file__).resolve().parents[1]
if str(PLUGINS_ROOT) not in sys.path:
    sys.path.append(str(PLUGINS_ROOT))

try:
    from plugins.utils.state_utils import is_co2_pollutant, normalize_state
except ImportError:  # pragma: no cover
    try:
        from utils.state_utils import is_co2_pollutant, normalize_state
    except ImportError:
        from state_utils import is_co2_pollutant, normalize_state

POUND_TO_KG = 0.45359237
SHORT_TON_TO_KG = 907.18474


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def convert_generation_to_mwh(generation: object, unit: Optional[str]) -> float:
    """
    Convert generation to MWh. EIA returns units like "thousand megawatthours".
    """
    raw_value = _to_float(generation, default=0.0)
    if unit is None:
        return raw_value

    unit_lower = str(unit).lower()
    if "thousand" in unit_lower:
        return raw_value * 1_000
    if "million" in unit_lower:
        return raw_value * 1_000_000
    return raw_value


def normalize_eia_generation(records: Iterable[Dict]) -> List[Dict[str, object]]:
    """
    Normalize EIA generation rows into a state/period/MWh structure.
    """
    totals: Dict[Tuple[str, str], float] = defaultdict(float)
    for row in records:
        state = normalize_state(row.get("stateDescription") or row.get("state"))
        period = row.get("period")
        if not state or not period:
            continue
        generation_mwh = convert_generation_to_mwh(
            generation=row.get("generation"),
            unit=row.get("generation-units") or row.get("generation_units"),
        )
        totals[(state, str(period))] += generation_mwh

    normalized = [
        {
            "state": state,
            "period": period,
            "total_generation_mwh": round(total, 6),
        }
        for (state, period), total in totals.items()
    ]
    return sorted(normalized, key=lambda x: (x["state"], x["period"]))


def _emissions_to_kg(value: object, unit: Optional[str]) -> float:
    raw_value = _to_float(value, default=0.0)
    if unit is None:
        return raw_value

    unit_lower = str(unit).lower()
    if "pound" in unit_lower or unit_lower in {"lb", "lbs"}:
        return raw_value * POUND_TO_KG
    if "short ton" in unit_lower or unit_lower.endswith("tons"):
        return raw_value * SHORT_TON_TO_KG
    if "metric ton" in unit_lower or "tonne" in unit_lower:
        return raw_value * 1000
    if unit_lower in {"kg", "kilogram", "kilograms"}:
        return raw_value
    return raw_value


def normalize_epa_emissions(records: Iterable[Dict]) -> List[Dict[str, object]]:
    """
    Normalize EPA emissions rows into a state/period/kg structure.
    Filters to CO2-equivalent pollutants only.
    """
    totals: Dict[Tuple[str, str], float] = defaultdict(float)
    for row in records:
        pollutant = (
            row.get("pollutant")
            or row.get("POLLUTANT")
            or row.get("pollutant_type")
            or row.get("POLLUTANT TYPE")
            or row.get("gas_code")
        )
        if pollutant and not is_co2_pollutant(pollutant):
            continue

        state = normalize_state(
            row.get("state_code")
            or row.get("STATE")
            or row.get("state")
            or row.get("state_name")
        )
        period = (
            row.get("period")
            or row.get("PERIOD")
            or row.get("year")
            or row.get("YEAR")
        )
        if not state or not period:
            continue

        emissions_kg = _emissions_to_kg(
            value=row.get("total_emission")
            or row.get("EMISSIONS")
            or row.get("emissions")
            or row.get("total_emission_kg")
            or row.get("co2e_emission")
            or 0,
            unit=row.get("UNIT OF MEASURE")
            or row.get("unit")
            or row.get("unit_of_measure")
            or row.get("emissions_units"),
        )
        totals[(state, str(period))] += emissions_kg

    normalized = [
        {
            "state": state,
            "period": period,
            "total_emissions_kg": round(total, 6),
        }
        for (state, period), total in totals.items()
    ]
    return sorted(normalized, key=lambda x: (x["state"], x["period"]))


def join_and_compute_intensity(
    generation: Iterable[Dict], emissions: Iterable[Dict]
) -> List[Dict[str, object]]:
    """
    Join normalized generation and emissions data by state/period and compute kg CO2 per MWh.
    Falls back to year-level matching when only a year is available on one side.
    """
    gen_lookup = {
        (row["state"], row["period"]): row["total_generation_mwh"]
        for row in generation
    }
    # Also keep year-level aggregates for looser matching when the periods differ.
    gen_by_year = defaultdict(float)
    for (state, period), mwh in gen_lookup.items():
        if len(str(period)) >= 4:
            gen_by_year[(state, str(period)[:4])] += mwh

    results: List[Dict[str, object]] = []
    for em in emissions:
        state = em["state"]
        period = str(em["period"])
        emissions_kg = em["total_emissions_kg"]

        generation_mwh = gen_lookup.get((state, period))
        if generation_mwh is None:
            # Try year-level match.
            generation_mwh = gen_by_year.get((state, period[:4]))

        if generation_mwh is None or math.isclose(generation_mwh, 0.0):
            continue

        results.append(
            {
                "state": state,
                "period": period,
                "total_generation_mwh": round(generation_mwh, 6),
                "total_emissions_kg": round(emissions_kg, 6),
                "emission_intensity_kg_per_mwh": round(
                    emissions_kg / generation_mwh, 6
                ),
            }
        )

    return sorted(results, key=lambda x: (x["state"], x["period"]))


def serialize_records(records: Iterable[Dict]) -> List[Dict]:
    """
    Ensure data is JSON serializable (e.g., convert datetimes).
    """
    serialized: List[Dict] = []
    for row in records:
        normalized_row = {}
        for key, value in row.items():
            if isinstance(value, datetime):
                normalized_row[key] = value.isoformat()
            else:
                normalized_row[key] = value
        serialized.append(normalized_row)
    return serialized
