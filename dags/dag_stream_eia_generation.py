from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.sensors.python import PythonSensor

import sys
PLUGINS_PATH = Path(__file__).resolve().parents[1] / "plugins"
if str(PLUGINS_PATH) not in sys.path:
    sys.path.append(str(PLUGINS_PATH))

from hooks.eia_hook import EIAHook


# Paths para dados streaming (separado do batch)
DATA_ROOT = Path("/opt/airflow/data")
STREAM_DIR = DATA_ROOT / "stream" / "eia"
STREAM_PROCESSED_DIR = DATA_ROOT / "processed"
STREAM_STATE_DIR = DATA_ROOT / "stream" / "state"


def _ensure_dirs() -> None:
    """Cria diretórios para dados streaming"""
    for path in (STREAM_DIR, STREAM_PROCESSED_DIR, STREAM_STATE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def _get_last_processed_timestamp() -> Optional[str]:
    """
    Lê o último timestamp processado para processamento incremental.
    
    Returns:
        Último timestamp (YYYY-MM-DDTHH) ou None se primeira execução
    """
    state_file = STREAM_STATE_DIR / "last_processed.json"
    
    if not state_file.exists():
        return None
    
    try:
        with state_file.open("r") as f:
            state = json.load(f)
            return state.get("last_timestamp")
    except Exception as e:
        print(f"[WARNING] Failed to read state file: {e}")
        return None


def _save_last_processed_timestamp(timestamp: str) -> None:
    """Salva o timestamp processado para próxima execução incremental"""
    _ensure_dirs()
    state_file = STREAM_STATE_DIR / "last_processed.json"
    
    state = {
        "last_timestamp": timestamp,
        "updated_at": datetime.now().isoformat(),
    }
    
    with state_file.open("w") as f:
        json.dump(state, f, indent=2)
    
    print(f"[INFO] Saved checkpoint: {timestamp}")


def _get_incremental_period() -> tuple[str, str]:
    """
    Calcula período incremental para buscar novos dados.
    
    Returns:
        (start_timestamp, end_timestamp) no formato YYYY-MM-DDTHH
    """
    last_processed = _get_last_processed_timestamp()
    now = datetime.now()
    
    if last_processed:
        # Processamento incremental: desde último checkpoint até agora
        start_dt = datetime.fromisoformat(last_processed.replace("T", " ").replace("Z", ""))
        start = start_dt.strftime("%Y-%m-%dT%H")
    else:
        # Primeira execução: últimas 24 horas
        start_dt = now - timedelta(hours=24)
        start = start_dt.strftime("%Y-%m-%dT%H")
    
    # Buscar até 1 hora atrás (dados recentes podem estar incompletos)
    end_dt = now - timedelta(hours=1)
    end = end_dt.strftime("%Y-%m-%dT%H")
    
    print(f"[INFO] Incremental period: {start} to {end}")
    return start, end


def check_new_data_available(**context) -> bool:
    """
    Sensor: verifica se há novos dados disponíveis para processar.
    
    Returns:
        True se há novos dados, False caso contrário
    """
    start, end = _get_incremental_period()
    
    start_dt = datetime.fromisoformat(start.replace("T", " "))
    end_dt = datetime.fromisoformat(end.replace("T", " "))
    
    # Se diferença < 1 hora, não há novos dados
    diff_hours = (end_dt - start_dt).total_seconds() / 3600
    
    if diff_hours < 1:
        print(f"[INFO] No new data yet (diff: {diff_hours:.1f}h)")
        return False
    
    print(f"[INFO] New data available ({diff_hours:.1f}h of data)")
    return True


def extract_recent_generation(**context) -> str:
    """
    Extrai dados de geração recentes (última hora ou desde último checkpoint).
    
    Pattern: Incremental upstream extraction
    """
    ti = context["ti"]
    dag_conf: Dict = context.get("dag_run").conf if context.get("dag_run") else {}
    
    # Período incremental
    start_ts, end_ts = _get_incremental_period()
    
    # Permitir override manual
    start_ts = dag_conf.get("start_ts", start_ts)
    end_ts = dag_conf.get("end_ts", end_ts)
    
    api_key = dag_conf.get("eia_api_key") or os.getenv("EIA_API_KEY") or Variable.get("EIA_API_KEY", default_var=None)
    hook = EIAHook(api_key=api_key)
    
    print(f"[INFO] Extracting generation data: {start_ts} to {end_ts}")
    
    try:
        # Buscar dados hourly/subhourly da API EIA
        records = hook.fetch_generation(
            start_period=start_ts,
            end_period=end_ts
        )
        
        if not records:
            print("[WARNING] No records returned for period")
            return ""
        
        print(f"[INFO] Extracted {len(records)} records")
        
    except Exception as e:
        print(f"[ERROR] Failed to extract data: {e}")
        raise
    
    # Salvar dados brutos (stream)
    _ensure_dirs()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = STREAM_DIR / f"generation_stream_{timestamp}.json"
    
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump({
            "extracted_at": datetime.now().isoformat(),
            "period_start": start_ts,
            "period_end": end_ts,
            "record_count": len(records),
            "records": records
        }, fp, indent=2)
    
    # Passar metadados via XCom
    ti.xcom_push(key="period_start", value=start_ts)
    ti.xcom_push(key="period_end", value=end_ts)
    ti.xcom_push(key="record_count", value=len(records))
    
    return str(output_path)


def transform_streaming_data(**context) -> str:
    """
    Transforma dados streaming em formato otimizado para dashboard.
    
    Pattern: Stream processing with aggregation
    """
    ti = context["ti"]
    
    # Ler dados extraídos
    raw_path = Path(ti.xcom_pull(task_ids="extract_recent_generation"))
    
    if not raw_path or not raw_path.exists():
        print("[WARNING] No data to transform")
        return ""
    
    with raw_path.open("r") as f:
        data = json.load(f)
    
    records = data.get("records", [])
    
    if not records:
        print("[WARNING] No records to process")
        return ""
    
    # Transformações para streaming:
    # 1. Agregar por estado
    # 2. Calcular totais por tipo de energia
    # 3. Identificar anomalias (variações > 20%)
    
    from collections import defaultdict
    
    state_totals = defaultdict(lambda: {"total_mwh": 0, "records": []})
    fuel_totals = defaultdict(float)
    
    for record in records:
        state = record.get("state") or record.get("respondent-name", "UNKNOWN")
        mwh = float(record.get("value", 0) or 0)
        fuel_type = record.get("fueltypeid", "UNKNOWN")
        
        state_totals[state]["total_mwh"] += mwh
        state_totals[state]["records"].append(record)
        fuel_totals[fuel_type] += mwh
    
    # Calcular métricas em tempo real
    real_time_metrics = {
        "timestamp": datetime.now().isoformat(),
        "period_start": data.get("period_start"),
        "period_end": data.get("period_end"),
        "total_generation_mwh": sum(fuel_totals.values()),
        "states_reporting": len(state_totals),
        "by_state": {
            state: info["total_mwh"] 
            for state, info in sorted(state_totals.items(), key=lambda x: x[1]["total_mwh"], reverse=True)
        },
        "by_fuel_type": dict(sorted(fuel_totals.items(), key=lambda x: x[1], reverse=True)),
        "top_5_states": list(sorted(state_totals.items(), key=lambda x: x[1]["total_mwh"], reverse=True)[:5])
    }
    
    # Salvar métricas processadas
    _ensure_dirs()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = STREAM_PROCESSED_DIR / f"metrics_realtime_{timestamp}.json"
    
    with output_path.open("w", encoding="utf-8") as fp:
        json.dump(real_time_metrics, fp, indent=2)
    
    print(f"[INFO] Processed {len(records)} records into real-time metrics")
    print(f"[INFO] Total generation: {real_time_metrics['total_generation_mwh']:.2f} MWh")
    
    return str(output_path)


def update_checkpoint(**context) -> None:
    """
    Atualiza checkpoint para próxima execução incremental.
    
    Pattern: Stateful stream processing
    """
    ti = context["ti"]
    
    period_end = ti.xcom_pull(task_ids="extract_recent_generation", key="period_end")
    
    if period_end:
        _save_last_processed_timestamp(period_end)
        print(f"[INFO] Checkpoint updated to: {period_end}")
    else:
        print("[WARNING] No period_end to checkpoint")

# Configuração do DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="dag_stream_eia_generation",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",  # ← Executa a cada hora
    catchup=False,
    max_active_runs=1,  # Evita runs concorrentes
    tags=["stream", "upstream", "eia", "real-time"],
    doc_md="",
) as dag:
    
    # Task 1: Sensor - Verifica se há novos dados
    check_data = PythonSensor(
        task_id="check_new_data",
        python_callable=check_new_data_available,
        mode="poke",
        timeout=300,  # 5 minutos
        poke_interval=60,  # Verifica a cada 1 minuto
    )
    
    # Task 2: Extração incremental
    extract = PythonOperator(
        task_id="extract_recent_generation",
        python_callable=extract_recent_generation,
    )
    
    # Task 3: Transformação streaming
    transform = PythonOperator(
        task_id="transform_streaming_data",
        python_callable=transform_streaming_data,
    )
    
    # Task 4: Atualizar checkpoint
    checkpoint = PythonOperator(
        task_id="update_checkpoint",
        python_callable=update_checkpoint,
    )
    
    # Fluxo: Sensor → Extract → Transform → Checkpoint
    check_data >> extract >> transform >> checkpoint
