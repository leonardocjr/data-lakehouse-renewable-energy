"""
Objective 1:
Correlate electricity generation (EIA) with CO2 emissions (EPA) and compute
emission intensity (kg CO2 per MWh) at the state level.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from utils.date_helpers import get_safe_data_period

# Ensure plugins directory is importable when the DAG is parsed.
PLUGINS_PATH = Path(__file__).resolve().parents[1] / "plugins"
if str(PLUGINS_PATH) not in sys.path:
    sys.path.append(str(PLUGINS_PATH))

from hooks.eia_hook import EIAHook
from hooks.epa_hook import EPAHook
from utils.transformations import (
    join_and_compute_intensity,
    normalize_eia_generation,
    normalize_epa_emissions,
    serialize_records,
)

DATA_ROOT = Path("/opt/airflow/data")
RAW_EIA_DIR = DATA_ROOT / "raw" / "eia"
RAW_EPA_DIR = DATA_ROOT / "raw" / "epa"
PROCESSED_DIR = DATA_ROOT / "processed" / "emission_intensity"


def _ensure_dirs() -> None:
    for path in (RAW_EIA_DIR, RAW_EPA_DIR, PROCESSED_DIR):
        path.mkdir(parents=True, exist_ok=True)

def _default_periods() -> tuple[str, str]:
    return get_safe_data_period()

def _default_month() -> str:
    # Return last full month in YYYY-MM to avoid empty current-month queries.
    today = datetime.utcnow().replace(day=1)
    last_month = today - timedelta(days=1)
    return last_month.strftime("%Y-%m")


def _prev_month(period: str) -> str:
    """Return previous month for a given YYYY-MM string."""
    dt = datetime.strptime(period, "%Y-%m")
    first = dt.replace(day=1)
    prev = first - timedelta(days=1)
    return prev.strftime("%Y-%m")


def extract_eia_generation(**context) -> str:
    ti = context["ti"]
    dag_run_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}
    
    start_period = dag_run_conf.get("start_period")
    end_period = dag_run_conf.get("end_period")

    if not start_period or not end_period:
        start_period, end_period = _default_periods()
        print(f"[INFO] Using safe data period: {start_period} to {end_period}")

    api_key = dag_run_conf.get("eia_api_key") or os.getenv("EIA_API_KEY") or Variable.get("EIA_API_KEY", default_var=None)
    hook = EIAHook(api_key=api_key)
    
    records = hook.fetch_generation(start_period=start_period, end_period=end_period)
    
    # Remover a lógica de retry com _prev_month() para manter consistência
    if not records:
        raise ValueError(
            f"EIA returned 0 records for period {start_period} to {end_period}. "
            f"This period should have data. Check API or use manual trigger with different period."
        )

    _ensure_dirs()
    output_path = RAW_EIA_DIR / f"eia_generation_{start_period}_{end_period}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2)
    
    ti.xcom_push(key="eia_periods", value={"start": start_period, "end": end_period})
    return str(output_path)


def extract_epa_emissions(**context) -> str:
    ti = context["ti"]
    dag_run_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}
    period = dag_run_conf.get("period") or _default_month()
    years = dag_run_conf.get("years")  # If None, fetch all and filter later via hook.
    state_code = dag_run_conf.get("state_code")  # Optional: restrict by state.

    hook = EPAHook()
    records = hook.fetch_emissions(
        state_code=state_code, years=years, limit=5000, offset=0
    )

    _ensure_dirs()
    years_in_data = sorted({int(row.get("year")) for row in records if row.get("year")})
    filename_years = years or (years_in_data if years_in_data else ["all"])
    output_path = RAW_EPA_DIR / f"epa_emissions_{filename_years[0]}_{filename_years[-1]}.json"
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, indent=2)
    ti.xcom_push(key="epa_years", value=years_in_data)
    return str(output_path)


def transform_and_join(**context) -> str:
    ti = context["ti"]
    dag_run_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}
    period = dag_run_conf.get("period") or datetime.utcnow().strftime("%Y-%m")
    start_period = dag_run_conf.get("start_period", period)
    end_period = dag_run_conf.get("end_period", period)

    eia_path = Path(ti.xcom_pull(task_ids="extract_eia_generation"))
    epa_path = Path(ti.xcom_pull(task_ids="extract_epa_emissions"))

    # Use actual periods used by EIA extraction if they were auto-aligned.
    eia_periods = ti.xcom_pull(task_ids="extract_eia_generation", key="eia_periods")
    if eia_periods:
        start_period = eia_periods.get("start", start_period)
        end_period = eia_periods.get("end", end_period)

    with eia_path.open(encoding="utf-8") as fp:
        eia_raw = json.load(fp)
    with epa_path.open(encoding="utf-8") as fp:
        epa_raw = json.load(fp)

    eia_normalized = normalize_eia_generation(eia_raw)
    epa_normalized = normalize_epa_emissions(epa_raw)
    intensity = join_and_compute_intensity(eia_normalized, epa_normalized)

    payload = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "start_period": start_period,
        "end_period": end_period,
        "source_files": {"eia": str(eia_path), "epa": str(epa_path)},
        "records": serialize_records(intensity),
    }

    _ensure_dirs()
    output_path = (
        PROCESSED_DIR / f"emission_intensity_{start_period}_{end_period}.json"
    )
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
    dag_id="dag_emission_intensity",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["batch", "eia", "epa", "emissions"],
) as dag:
    extract_epa = PythonOperator(
        task_id="extract_epa_emissions",
        python_callable=extract_epa_emissions,
    )

    extract_eia = PythonOperator(
        task_id="extract_eia_generation",
        python_callable=extract_eia_generation,
    )

    transform_join = PythonOperator(
        task_id="transform_and_join",
        python_callable=transform_and_join,
    )

    validate = PythonOperator(
        task_id="validate_period",
        python_callable=validate_period_consistency,
    )

    extract_epa >> extract_eia >> transform_join >> validate
