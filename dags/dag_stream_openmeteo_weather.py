from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
PLUGINS_PATH = Path(__file__).resolve().parents[1] / "plugins"
if str(PLUGINS_PATH) not in sys.path:
    sys.path.append(str(PLUGINS_PATH))

from hooks.openmeteo_hook import OpenMeteoHook


DATA_ROOT = Path("/opt/airflow/data")
STREAM_DIR = DATA_ROOT / "raw" / "openmeteo"
STREAM_PROCESSED_DIR = DATA_ROOT / "processed" / "stream_openmeteo"


DEFAULT_LOCATIONS = {
    "CA": {"lat": 36.7783, "lon": -119.4179, "name": "California"},
    "TX": {"lat": 31.9686, "lon": -99.9018, "name": "Texas"},
    "NY": {"lat": 42.1657, "lon": -74.9481, "name": "New York"},
    "FL": {"lat": 27.9944, "lon": -81.7603, "name": "Florida"},
    "AZ": {"lat": 34.0489, "lon": -111.0937, "name": "Arizona"},
}


def _ensure_dirs() -> None:
    for path in (STREAM_DIR, STREAM_PROCESSED_DIR):
        path.mkdir(parents=True, exist_ok=True)


def _get_recent_period() -> tuple[str, str]:
    now = datetime.now()
    
    start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # start_date = "2024-01-01"
    # end_date = "2024-01-07"
    
    print(f"[INFO] Period: {start_date} to {end_date}")
    
    return start_date, end_date


def extract_weather_stream(**context) -> str:
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}
    
    start_date, end_date = _get_recent_period()
    
    start_date = dag_conf.get("start_date", start_date)
    end_date = dag_conf.get("end_date", end_date)
    
    locations = dag_conf.get("locations", DEFAULT_LOCATIONS)
    
    hook = OpenMeteoHook()
    
    print(f"[INFO] Extracting weather: {start_date} to {end_date}")
    print(f"[INFO] Locations: {len(locations)}")
    
    all_data = {}
    
    for state, coords in locations.items():
        try:
            print(f"[INFO] Fetching {state} ({coords['name']})")
            
            weather_data = hook.fetch_hourly_weather(
                latitude=coords["lat"],
                longitude=coords["lon"],
                start_date=start_date,
                end_date=end_date
            )
            
            all_data[state] = {
                "location": coords,
                "weather": weather_data
            }
            
        except Exception as e:
            print(f"[ERROR] Failed {state}: {e}")
            continue
    
    _ensure_dirs()
    output_path = STREAM_DIR / "weather_stream_latest.json"
    
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump({
            "extracted_at": datetime.now().isoformat(),
            "period_start": start_date,
            "period_end": end_date,
            "location_count": len(all_data),
            "data": all_data
        }, fp, indent=2)
    
    print(f"[INFO] Updated {output_path} with {len(all_data)} locations")
    
    return str(output_path)


def transform_weather_stream(**context) -> str:
    ti = context["ti"]
    
    raw_path_str = ti.xcom_pull(task_ids="extract_weather_stream")
    
    if not raw_path_str:
        print("[WARNING] No data")
        return ""
    
    raw_path = Path(raw_path_str)
    
    if not raw_path.exists():
        print(f"[ERROR] Path not found: {raw_path}")
        return ""
    
    with raw_path.open("r") as f:
        data = json.load(f)
    
    location_data = data.get("data", {})
    
    if not location_data:
        print("[WARNING] No locations")
        return ""
    
    print(f"[INFO] Processing {len(location_data)} locations")
    
    aggregated = {
        "timestamp": datetime.now().isoformat(),
        "period_start": data.get("period_start"),
        "period_end": data.get("period_end"),
        "locations": {}
    }
    
    for state, info in location_data.items():
        weather = info.get("weather", {})
        hourly = weather.get("hourly", {})
        
        if not hourly:
            continue
        
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        ghi = hourly.get("shortwave_radiation", [])
        
        if not times:
            continue
        
        avg_temp = sum(temps) / len(temps) if temps else 0
        avg_ghi = sum(ghi) / len(ghi) if ghi else 0
        max_temp = max(temps) if temps else 0
        min_temp = min(temps) if temps else 0
        
        aggregated["locations"][state] = {
            "name": info["location"]["name"],
            "hours_recorded": len(times),
            "avg_temperature_c": round(avg_temp, 2),
            "max_temperature_c": round(max_temp, 2),
            "min_temperature_c": round(min_temp, 2),
            "avg_ghi_w_m2": round(avg_ghi, 2)
        }
    
    _ensure_dirs()
    output_path = STREAM_PROCESSED_DIR / "weather_metrics_latest.json"
    
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(aggregated, fp, indent=2)
    
    print(f"[INFO] Updated {output_path} with {len(aggregated['locations'])} locations")
    
    return str(output_path)


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="dag_stream_openmeteo_weather",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="*/10 * * * *",  # A cada 10 minutos
    catchup=False,
    max_active_runs=1,
    tags=["stream", "upstream", "openmeteo", "weather"],
) as dag:
    
    extract = PythonOperator(
        task_id="extract_weather_stream",
        python_callable=extract_weather_stream,
    )
    
    transform = PythonOperator(
        task_id="transform_weather_stream",
        python_callable=transform_weather_stream,
    )
    
    extract >> transform