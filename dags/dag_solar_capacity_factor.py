"""
Objective 2:
Correlate solar generation (EIA), irradiance/temperature (Open-Meteo), and
technical capacity (EIA inventory + NREL GHI) to compute solar capacity factor by state/month.
"""

from __future__ import annotations

import calendar
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.date_helpers import get_safe_data_period

# Ensure plugins directory is importable when the DAG is parsed.
PLUGINS_PATH = Path(__file__).resolve().parents[1] / "plugins"
if str(PLUGINS_PATH) not in sys.path:
    sys.path.append(str(PLUGINS_PATH))

from hooks.eia_hook import EIAHook
from hooks.nrel_hook import NRELHook
from hooks.openmeteo_hook import OpenMeteoHook
from utils.solar_transformations import (
    derive_spatial_weights,
    merge_capacity_factor,
    normalize_generator_capacity,
    normalize_nrel_ghi,
    normalize_openmeteo_hourly,
    normalize_rto_generation,
)
from utils.transformations import serialize_records

DATA_ROOT = Path("/opt/airflow/data")
RAW_EIA_DIR = DATA_ROOT / "raw" / "eia"
RAW_OPENMETEO_DIR = DATA_ROOT / "raw" / "openmeteo"
RAW_NREL_DIR = DATA_ROOT / "raw" / "nrel"
PROCESSED_DIR = DATA_ROOT / "processed" / "solar_capacity_factor"

DEFAULT_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM",
    "NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT","VA","WA",
    "WV","WI","WY"
]

def _ensure_dirs() -> None:
    for path in (RAW_EIA_DIR, RAW_OPENMETEO_DIR, RAW_NREL_DIR, PROCESSED_DIR):
        path.mkdir(parents=True, exist_ok=True)


def _default_periods() -> tuple[str, str]:
        return get_safe_data_period()
"""
        today = datetime.now()        
        if today.month == 1:
            last_month = today.replace(year=today.year - 1, month=12, day=1)
        else:
            last_month = today.replace(month=today.month - 1, day=1)
        return last_month.strftime("%Y-%m"), last_month.strftime("%Y-%m")
"""

def _month_range_bounds(start_period: str, end_period: str) -> Dict[str, str]:
    start_dt = datetime.strptime(start_period, "%Y-%m")
    end_dt = datetime.strptime(end_period, "%Y-%m")
    if end_dt < start_dt:
        raise ValueError("end_period must be greater than or equal to start_period.")
    start_date = f"{start_dt:%Y-%m}-01"
    last_day = calendar.monthrange(end_dt.year, end_dt.month)[1]
    end_date = f"{end_dt.year:04d}-{end_dt.month:02d}-{last_day:02d}"
    return {
        "start_ts": f"{start_date}T00",
        "end_ts": f"{end_date}T23",
        "start_date": start_date,
        "end_date": end_date,
    }


def _select_respondents_from_weights(
    states: Iterable[str], respondent_weights: Dict[str, Dict[str, float]]
) -> List[str]:
    wanted = set()
    state_set = set(states)
    for respondent, state_weights in respondent_weights.items():
        if state_set.intersection(state_weights.keys()):
            wanted.add(respondent)
    return sorted(wanted)


def extract_generator_capacity(**context) -> str:
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}

    states = dag_conf.get("states") or DEFAULT_STATES
    start_period = dag_conf.get("start_period")
    end_period = dag_conf.get("end_period")
    if not start_period or not end_period:
        start_period, end_period = _default_periods()

    api_key = dag_conf.get("eia_api_key") or os.getenv("EIA_API_KEY") or Variable.get("EIA_API_KEY", default_var=None)
    hook = EIAHook(api_key=api_key)
    records = hook.fetch_generator_capacity(
        states=states, start_period=start_period, end_period=end_period
    )

    _ensure_dirs()
    output_path = RAW_EIA_DIR / f"generator_capacity_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2)

    ti.xcom_push(key="states", value=states)
    ti.xcom_push(key="periods", value={"start": start_period, "end": end_period})
    return str(output_path)


def derive_spatial_weights_task(**context) -> str:
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}

    states = dag_conf.get("states") or ti.xcom_pull(key="states") or DEFAULT_STATES
    periods = ti.xcom_pull(key="periods") or {}
    start_period = dag_conf.get("start_period") or periods.get("start")
    end_period = dag_conf.get("end_period") or periods.get("end")
    if not start_period or not end_period:
        start_period, end_period = _default_periods()

    capacity_path = Path(ti.xcom_pull(task_ids="extract_generator_capacity"))
    with capacity_path.open(encoding="utf-8") as fp:
        capacity_raw = json.load(fp)

    state_coords_all, respondent_weights_all = derive_spatial_weights(capacity_raw)
    state_set = set(states)

    state_coords = {s: v for s, v in state_coords_all.items() if s in state_set}
    respondent_weights: Dict[str, Dict[str, float]] = {}
    for respondent, weights in respondent_weights_all.items():
        filtered = {s: w for s, w in weights.items() if s in state_set}
        if filtered:
            respondent_weights[respondent] = filtered

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "state_coords": state_coords,
        "respondent_weights": respondent_weights,
    }

    _ensure_dirs()
    output_path = PROCESSED_DIR / f"spatial_weights_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)

    ti.xcom_push(key="state_coords", value=state_coords)
    ti.xcom_push(key="respondent_weights", value=respondent_weights)
    return str(output_path)


def extract_rto_generation(**context) -> str:
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}

    states = dag_conf.get("states") or ti.xcom_pull(key="states") or DEFAULT_STATES
    periods = ti.xcom_pull(key="periods") or {}
    start_period = dag_conf.get("start_period") or periods.get("start")
    end_period = dag_conf.get("end_period") or periods.get("end")
    if not start_period or not end_period:
        start_period, end_period = _default_periods()
    bounds = _month_range_bounds(start_period, end_period)

    respondent_weights = (
        dag_conf.get("respondent_weights")
        or ti.xcom_pull(task_ids="derive_spatial_weights", key="respondent_weights")
        or {}
    )
    respondents = dag_conf.get("respondents") or _select_respondents_from_weights(
        states, respondent_weights
    )
    api_key = dag_conf.get("eia_api_key") or os.getenv("EIA_API_KEY")
    hook = EIAHook(api_key=api_key)
    records: List[Dict] = []
    if respondents:
        records = hook.fetch_rto_solar_generation(
            respondents=respondents,
            start_period=bounds["start_ts"],
            end_period=bounds["end_ts"],
        )

    _ensure_dirs()
    output_path = RAW_EIA_DIR / f"rto_generation_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2)

    ti.xcom_push(key="respondents", value=respondents)
    return str(output_path)


def extract_openmeteo(**context) -> str:
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}

    states = dag_conf.get("states") or ti.xcom_pull(key="states") or DEFAULT_STATES
    periods = ti.xcom_pull(key="periods") or {}
    start_period = dag_conf.get("start_period") or periods.get("start")
    end_period = dag_conf.get("end_period") or periods.get("end")

    if not start_period or not end_period:
        start_period, end_period = _default_periods()

    bounds = _month_range_bounds(start_period, end_period)
    state_coords = ti.xcom_pull(task_ids="derive_spatial_weights", key="state_coords") or {}

    hook = OpenMeteoHook()
    payloads: Dict[str, Dict] = {}
    total_states = len(states)

    for idx, state in enumerate(states, 1):
        coords = state_coords.get(state)
        if not coords:
            continue
        
        try:
            print(f"Fetching weather for {state} ({idx}/{total_states})")
            payloads[state] = hook.fetch_hourly_weather(
                latitude=coords["lat"],
                longitude=coords["lon"],
                start_date=bounds["start_date"],
                end_date=bounds["end_date"],
            )
            
            # Delay de 0.5s entre requisições para evitar rate limit
            if idx < total_states:
                time.sleep(0.5)
                
        except Exception as e:
            print(f"Failed to fetch weather for {state}: {e}")
            continue

    _ensure_dirs()
    output_path = RAW_OPENMETEO_DIR / f"openmeteo_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(payloads, fp, indent=2)
    return str(output_path)


def extract_nrel(**context) -> str:
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}

    states = dag_conf.get("states") or ti.xcom_pull(key="states") or DEFAULT_STATES
    periods = ti.xcom_pull(key="periods") or {}
    start_period = dag_conf.get("start_period") or periods.get("start")
    end_period = dag_conf.get("end_period") or periods.get("end")
    if not start_period or not end_period:
        start_period, end_period = _default_periods()

    state_coords = ti.xcom_pull(task_ids="derive_spatial_weights", key="state_coords") or {}

    api_key = dag_conf.get("nrel_api_key") or os.getenv("NREL_API_KEY") or Variable.get("NREL_API_KEY", default_var=None)
    hook = NRELHook(api_key=api_key)

    records: List[Dict] = []
    for state in states:
        coords = state_coords.get(state)
        if not coords:
            continue
        payload = hook.fetch_solar_resource(
            latitude=coords["lat"], longitude=coords["lon"]
        )
        payload["state"] = state
        records.append(payload)

    _ensure_dirs()
    output_path = RAW_NREL_DIR / f"nrel_solar_resource_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2)
    return str(output_path)


def transform_capacity_factor(**context) -> str:
    ti = context["ti"]
    periods = ti.xcom_pull(key="periods") or {}
    start_period = periods.get("start") or _default_periods()[0]
    end_period = periods.get("end") or _default_periods()[1]
    states = ti.xcom_pull(key="states") or DEFAULT_STATES

    capacity_path = Path(ti.xcom_pull(task_ids="extract_generator_capacity"))
    generation_path = Path(ti.xcom_pull(task_ids="extract_rto_generation"))
    weather_path = Path(ti.xcom_pull(task_ids="extract_openmeteo"))
    nrel_path = Path(ti.xcom_pull(task_ids="extract_nrel"))

    with capacity_path.open(encoding="utf-8") as fp:
        capacity_raw = json.load(fp)
    with generation_path.open(encoding="utf-8") as fp:
        generation_raw = json.load(fp)
    with weather_path.open(encoding="utf-8") as fp:
        weather_raw = json.load(fp)
    with nrel_path.open(encoding="utf-8") as fp:
        nrel_raw = json.load(fp)

    respondent_weights = (
        ti.xcom_pull(task_ids="derive_spatial_weights", key="respondent_weights") or {}
    )
    state_set = set(states)

    capacity = normalize_generator_capacity(capacity_raw)
    capacity = [row for row in capacity if row.get("state") in state_set]
    generation = normalize_rto_generation(generation_raw, respondent_weights)
    generation = [row for row in generation if row.get("state") in state_set]

    weather_records: List[Dict[str, object]] = []
    for state, payload in weather_raw.items():
        weather_records.extend(normalize_openmeteo_hourly(payload, state))
    weather_records = [row for row in weather_records if row.get("state") in state_set]

    ghi = normalize_nrel_ghi(nrel_raw)
    ghi = [row for row in ghi if row.get("state") in state_set]
    merged = merge_capacity_factor(generation, capacity, weather_records, ghi)

    payload = {
        "generated_at": datetime.now().isoformat() + "Z",
        "start_period": start_period,
        "end_period": end_period,
        "source_files": {
            "eia_capacity": str(capacity_path),
            "eia_generation": str(generation_path),
            "openmeteo": str(weather_path),
            "nrel": str(nrel_path),
        },
        "records": serialize_records(merged),
    }

    _ensure_dirs()
    output_path = PROCESSED_DIR / f"solar_capacity_factor_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)
    return str(output_path)

def validate_period_consistency(**context):
    """Valida que o período usado é consistente com outros DAGs"""
    ti = context["ti"]
    periods = ti.xcom_pull(key="periods") or {}
    start = periods.get("start")
    end = periods.get("end")
    
    safe_start, safe_end = get_safe_data_period()
    
    if start == safe_start and end == safe_end:
        print(f"✅ Period validation passed: {start} to {end}")
        return True
    else:
        print(f"⚠️ WARNING: Using custom period {start} to {end} (safe: {safe_start} to {safe_end})")
        return True

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_solar_capacity_factor",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["eia", "openmeteo", "nrel", "solar", "capacity-factor"],
) as dag:
    extract_capacity = PythonOperator(
        task_id="extract_generator_capacity",
        python_callable=extract_generator_capacity,
    )

    derive_spatial = PythonOperator(
        task_id="derive_spatial_weights",
        python_callable=derive_spatial_weights_task,
    )

    extract_generation = PythonOperator(
        task_id="extract_rto_generation",
        python_callable=extract_rto_generation,
    )

    extract_weather = PythonOperator(
        task_id="extract_openmeteo",
        python_callable=extract_openmeteo,
    )

    extract_nrel_task = PythonOperator(
        task_id="extract_nrel",
        python_callable=extract_nrel,
    )

    compute_capacity_factor = PythonOperator(
        task_id="transform_capacity_factor",
        python_callable=transform_capacity_factor,
    )

    validate = PythonOperator(
        task_id="validate_period",
        python_callable=validate_period_consistency,
    )

    extract_capacity >> derive_spatial >> [extract_generation, extract_weather, extract_nrel_task] >> compute_capacity_factor >> validate
