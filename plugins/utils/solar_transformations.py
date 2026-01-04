"""
Helpers to normalize EIA solar data, Open-Meteo weather, and compute capacity factor joins.
"""

from __future__ import annotations

import calendar
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

# Ensure plugins root is on sys.path for relative imports when loaded by Airflow.
PLUGINS_ROOT = Path(__file__).resolve().parents[1]
import sys

if str(PLUGINS_ROOT) not in sys.path:
    sys.path.append(str(PLUGINS_ROOT))

try:
    from plugins.utils.state_utils import normalize_state
except ImportError:  # pragma: no cover
    try:
        from utils.state_utils import normalize_state
    except ImportError:
        from state_utils import normalize_state


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_iso8601(value: object) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def normalize_generator_capacity(records: Iterable[Dict]) -> List[Dict[str, object]]:
    """
    Normalize EIA generator inventory rows into state/period/capacity (MW).
    """
    totals: Dict[Tuple[str, str], float] = defaultdict(float)
    for row in records:
        state = normalize_state(
            row.get("stateid") or row.get("state") or row.get("stateName")
        )
        period = row.get("period")
        if not state or not period:
            continue
        capacity = _to_float(
            row.get("net-summer-capacity-mw")
            or row.get("net_summer_capacity_mw")
            or row.get("capacity_mw")
            or 0
        )
        totals[(state, str(period))] += capacity

    normalized = [
        {"state": state, "period": period, "capacity_mw": round(total, 6)}
        for (state, period), total in totals.items()
    ]
    return sorted(normalized, key=lambda x: (x["state"], x["period"]))


def derive_spatial_weights(
    records: Iterable[Dict],
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, Dict[str, float]]]:
    """
    Derive capacity-weighted state centroids (lat/lon) and balancing authority
    state weights from raw EIA-860/860M generator inventory rows.
    """
    state_aggs: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"cap": 0.0, "lat_sum": 0.0, "lon_sum": 0.0}
    )
    ba_state_cap: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for row in records:
        energy_code = str(
            row.get("energy_source_code")
            or row.get("energy-source-desc")
            or row.get("energy_source")
            or ""
        ).upper()
        if energy_code and energy_code != "SUN":
            continue

        state = normalize_state(
            row.get("stateid") or row.get("state") or row.get("stateName")
        )
        if not state:
            continue

        cap = _to_float(
            row.get("net-summer-capacity-mw")
            or row.get("net_summer_capacity_mw")
            or row.get("capacity_mw")
            or 0
        )
        if math.isclose(cap, 0.0):
            continue

        try:
            lat = float(row.get("latitude"))
            lon = float(row.get("longitude"))
        except (TypeError, ValueError):
            lat = lon = None
        if lat is not None and lon is not None:
            agg = state_aggs[state]
            agg["cap"] += cap
            agg["lat_sum"] += lat * cap
            agg["lon_sum"] += lon * cap

        ba = (
            str(
                row.get("balancing_authority_code")
                or row.get("balancing_authority")
                or row.get("respondent")
                or ""
            )
            .strip()
            .upper()
        )
        if ba:
            ba_state_cap[ba][state] += cap

    state_coords: Dict[str, Dict[str, float]] = {}
    for state, agg in state_aggs.items():
        if agg["cap"] <= 0:
            continue
        state_coords[state] = {
            "lat": round(agg["lat_sum"] / agg["cap"], 6),
            "lon": round(agg["lon_sum"] / agg["cap"], 6),
        }

    ba_state_weights: Dict[str, Dict[str, float]] = {}
    for ba, states_caps in ba_state_cap.items():
        total = sum(cap for cap in states_caps.values() if cap > 0)
        if math.isclose(total, 0.0):
            continue
        weights = {
            state: round(cap / total, 6)
            for state, cap in states_caps.items()
            if cap > 0
        }
        if weights:
            ba_state_weights[ba] = weights

    return state_coords, ba_state_weights


def normalize_rto_generation(
    records: Iterable[Dict],
    respondent_state_weights: Dict[str, Dict[str, float]],
) -> List[Dict[str, object]]:
    """
    Normalize hourly balancing-authority generation into state/month buckets using
    respondent->state capacity weights.
    """
    totals: Dict[Tuple[str, str], float] = defaultdict(float)
    for row in records:
        respondent = (
            str(row.get("respondent") or row.get("respondentid") or "")
            .strip()
            .upper()
        )
        weights = respondent_state_weights.get(respondent)
        if not weights:
            continue

        dt = _parse_iso8601(row.get("period"))
        if not dt:
            continue
        period = dt.strftime("%Y-%m")

        generation_mwh = _to_float(row.get("value") or row.get("generation"))

        total_weight = sum(w for w in weights.values() if w > 0)
        if math.isclose(total_weight, 0.0):
            continue

        for state, weight in weights.items():
            if weight <= 0:
                continue
            scaled = generation_mwh * (weight / total_weight)
            totals[(state, period)] += scaled

    normalized = [
        {
            "state": state,
            "period": period,
            "total_generation_mwh": round(total, 6),
        }
        for (state, period), total in totals.items()
    ]
    return sorted(normalized, key=lambda x: (x["state"], x["period"]))


def normalize_openmeteo_hourly(payload: Dict, state: str) -> List[Dict[str, object]]:
    """
    Aggregate Open-Meteo hourly data into monthly buckets with irradiance and temperature summaries.
    """
    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    radiation = hourly.get("direct_radiation") or []
    temps = hourly.get("temperature_2m") or []
    clouds = hourly.get("cloudcover") or []

    buckets: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"radiation_mwh_m2": 0.0, "temp_sum": 0.0, "cloud_sum": 0.0, "count": 0}
    )

    for ts, rad, temp, cloud in zip(times, radiation, temps, clouds):
        dt = _parse_iso8601(ts)
        if not dt:
            continue
        period = dt.strftime("%Y-%m")
        bucket = buckets[period]
        bucket["radiation_mwh_m2"] += _to_float(rad) / 1_000_000.0  # W/m2 -> MWh/m2
        bucket["temp_sum"] += _to_float(temp)
        bucket["cloud_sum"] += _to_float(cloud)
        bucket["count"] += 1

    normalized: List[Dict[str, object]] = []
    for period, vals in buckets.items():
        count = max(int(vals["count"]), 1)
        avg_temp = vals["temp_sum"] / count
        avg_cloud = vals["cloud_sum"] / count
        available_hours = vals["radiation_mwh_m2"] * 1000  # kWh/m2 ~ peak sun hours
        normalized.append(
            {
                "state": state,
                "period": period,
                "radiation_mwh_m2": round(vals["radiation_mwh_m2"], 6),
                "avg_temperature_c": round(avg_temp, 3),
                "avg_cloud_cover_pct": round(avg_cloud, 3),
                "available_solar_hours": round(available_hours, 6),
            }
        )

    return sorted(normalized, key=lambda x: (x["state"], x["period"]))


def normalize_nrel_ghi(records: Iterable[Dict]) -> List[Dict[str, object]]:
    """
    Normalize NREL solar resource payloads into state-level average GHI (kWh/m2/day).
    """
    normalized: List[Dict[str, object]] = []
    for row in records:
        state = normalize_state(row.get("state"))
        if not state:
            continue
        avg_ghi = row.get("avg_ghi_kwh_m2_day")
        outputs = row.get("outputs")
        if avg_ghi is None and outputs:
            avg = outputs.get("avg_ghi")
            if isinstance(avg, dict):
                avg_ghi = avg.get("annual")
            else:
                avg_ghi = avg
        if avg_ghi is None:
            continue
        normalized.append(
            {"state": state, "avg_ghi_kwh_m2_day": round(_to_float(avg_ghi), 6)}
        )
    return sorted(normalized, key=lambda x: x["state"])


def merge_capacity_factor(
    generation: Iterable[Dict],
    capacity: Iterable[Dict],
    weather: Iterable[Dict],
    ghi: Iterable[Dict],
) -> List[Dict[str, object]]:
    """
    Join generation, capacity, irradiance, and GHI to compute solar capacity factor per state/month.
    """

    def _hours_in_month(period: str) -> int:
        try:
            dt = datetime.strptime(period, "%Y-%m")
        except ValueError:
            return 0
        days = calendar.monthrange(dt.year, dt.month)[1]
        return days * 24

    capacity_by_period: Dict[Tuple[str, str], float] = {}
    latest_capacity: Dict[str, Tuple[str, float]] = {}
    for row in capacity:
        state = row.get("state")
        period = str(row.get("period"))
        if not state or not period:
            continue
        cap = _to_float(row.get("capacity_mw"))
        capacity_by_period[(state, period)] = cap
        if (state not in latest_capacity) or (period > latest_capacity[state][0]):
            latest_capacity[state] = (period, cap)

    weather_lookup = {
        (row["state"], str(row["period"])): row for row in weather if row.get("state")
    }
    ghi_lookup = {row["state"]: _to_float(row["avg_ghi_kwh_m2_day"]) for row in ghi}

    results: List[Dict[str, object]] = []
    for gen in generation:
        state = gen.get("state")
        period = str(gen.get("period"))
        if not state or not period:
            continue

        hours_month = _hours_in_month(period)
        if hours_month <= 0:
            continue

        gen_mwh = _to_float(gen.get("total_generation_mwh"))
        cap = capacity_by_period.get((state, period))
        if cap is None and state in latest_capacity:
            cap = latest_capacity[state][1]
        if cap is None or math.isclose(cap, 0.0):
            continue

        weather_row = weather_lookup.get((state, period))
        solar_hours = (
            weather_row.get("available_solar_hours", 0.0) if weather_row else 0.0
        )

        denominator = cap * hours_month
        if math.isclose(denominator, 0.0):
            continue

        scf = gen_mwh / denominator
        specific_yield = gen_mwh / cap if not math.isclose(cap, 0.0) else 0.0

        record = {
            "state": state,
            "period": period,
            "total_generation_mwh": round(gen_mwh, 6),
            "capacity_mw": round(cap, 6),
            "hours_in_month": hours_month,
            "available_solar_hours": round(solar_hours, 6),
            "specific_yield_mwh_per_mw": round(specific_yield, 6),
            "solar_capacity_factor": round(scf, 6),
        }

        if weather_row:
            record["radiation_mwh_m2"] = round(
                weather_row.get("radiation_mwh_m2", 0.0), 6
            )
            record["avg_temperature_c"] = round(
                weather_row.get("avg_temperature_c", 0.0), 3
            )
            record["avg_cloud_cover_pct"] = round(
                weather_row.get("avg_cloud_cover_pct", 0.0), 3
            )

        if state in ghi_lookup:
            record["avg_ghi_kwh_m2_day"] = round(ghi_lookup[state], 6)

        results.append(record)

    return sorted(results, key=lambda x: (x["state"], x["period"]))
