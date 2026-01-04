"""
Lightweight EIA hook to retrieve electric power operational data.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import requests
from airflow.hooks.base import BaseHook


class EIAHook(BaseHook):
    """
    Fetches electricity generation data from the EIA v2 API.
    """

    conn_name_attr = "eia_conn_id"
    default_conn_name = "eia_default"
    base_url = (
        "https://api.eia.gov/v2/electricity/electric-power-operational-data/data"
    )
    generator_capacity_url = (
        "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/"
    )
    rto_generation_url = "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data/"

    def __init__(
        self,
        api_key: Optional[str] = None,
        eia_conn_id: Optional[str] = None,
        request_timeout: int = 60,
    ) -> None:
        super().__init__()
        self.api_key = api_key or self._resolve_api_key(eia_conn_id)
        self.request_timeout = request_timeout

    def _resolve_api_key(self, conn_id: Optional[str]) -> Optional[str]:
        if conn_id:
            conn = self.get_connection(conn_id)
            if conn.password:
                return conn.password
        return os.getenv("EIA_API_KEY")

    def fetch_generation(
        self,
        start_period: str,
        end_period: str,
        frequency: str = "monthly",
        length: int = 5000,
        facets: Optional[Dict[str, str]] = None,
        offset_start: int = 0,
    ) -> List[Dict]:
        """
        Pull generation data between start_period and end_period (inclusive).
        Handles pagination using the `length` and `offset` params.
        """
        if not self.api_key:
            raise ValueError("EIA API key not configured (set EIA_API_KEY or a conn).")

        params: Dict[str, object] = {
            "api_key": self.api_key,
            "frequency": frequency,
            "start": start_period,
            "end": end_period,
            "length": length,
            "offset": offset_start,
            "data[0]": "generation",
        }

        if facets:
            for idx, (key, value) in enumerate(facets.items()):
                params[f"facets[{key}]"] = value

        results: List[Dict] = []
        offset = offset_start
        while True:
            params["offset"] = offset
            resp = requests.get(
                self.base_url, params=params, timeout=self.request_timeout
            )
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("response", {}).get("data", []) or []
            total = int(payload.get("response", {}).get("total", len(data)))
            results.extend(data)

            if len(results) >= total or not data:
                break
            offset += len(data)

        return results

    def _fetch_dataset(
        self,
        base_url: str,
        data_fields: List[str],
        start: Optional[str] = None,
        end: Optional[str] = None,
        frequency: Optional[str] = None,
        facets: Optional[Dict[str, object]] = None,
        sort: Optional[List[Dict[str, str]]] = None,
        length: int = 5000,
        offset_start: int = 0,
    ) -> List[Dict]:
        """
        Generic paginator for EIA v2 datasets.
        """
        if not self.api_key:
            raise ValueError("EIA API key not configured (set EIA_API_KEY or a conn).")

        params: List[tuple[str, object]] = [("api_key", self.api_key)]
        if frequency:
            params.append(("frequency", frequency))
        if start:
            params.append(("start", start))
        if end:
            params.append(("end", end))
        params.append(("length", length))

        for idx, field in enumerate(data_fields):
            params.append((f"data[{idx}]", field))

        if facets:
            for key, value in facets.items():
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        params.append((f"facets[{key}][]", item))
                else:
                    params.append((f"facets[{key}][]", value))

        if sort:
            for idx, rule in enumerate(sort):
                column = rule.get("column")
                direction = rule.get("direction", "desc")
                if column:
                    params.append((f"sort[{idx}][column]", column))
                    params.append((f"sort[{idx}][direction]", direction))

        results: List[Dict] = []
        offset = offset_start
        while True:
            current_params = [(k, v) for (k, v) in params if k != "offset"]
            current_params.append(("offset", offset))
            resp = requests.get(
                base_url, params=current_params, timeout=self.request_timeout
            )
            try:
                resp.raise_for_status()
            except requests.HTTPError as exc:  # surface API error body for visibility
                body = resp.text
                snippet = body[:500] + ("..." if len(body) > 500 else "")
                raise requests.HTTPError(
                    f"{exc} | url={resp.url} | body={snippet}"
                ) from exc
            payload = resp.json()
            data = payload.get("response", {}).get("data", []) or []
            total = int(payload.get("response", {}).get("total", len(data)))
            results.extend(data)
            if len(results) >= total or not data:
                break
            offset += len(data)

        return results

    def fetch_generator_capacity(
        self,
        states: List[str],
        start_period: str,
        end_period: str,
        length: int = 5000,
        fallback_months: int = 12,
    ) -> List[Dict]:
        """
        Retrieve net summer capacity (MW) for solar generators by state.
        Falls back to the most recent prior month (up to `fallback_months`)
        when the requested window has no data yet (common for not-yet-released months).
        """
        facets = {"stateid": states, "energy_source_code": ["SUN"]}
        sort = [{"column": "period", "direction": "desc"}]
        data_fields = [
            "net-summer-capacity-mw",
            "latitude",
            "longitude",
        ]
        results = self._fetch_dataset(
            base_url=self.generator_capacity_url,
            data_fields=data_fields,
            start=start_period,
            end=end_period,
            frequency="monthly",
            facets=facets,
            sort=sort,
            length=length,
        )

        if results:
            return results

        anchor_period = end_period or start_period

        def _iter_prior_months(period: str, months_back: int) -> Iterable[str]:
            try:
                dt = datetime.strptime(period, "%Y-%m")
            except ValueError:
                return
            year, month = dt.year, dt.month
            for _ in range(months_back):
                month -= 1
                if month == 0:
                    month = 12
                    year -= 1
                yield f"{year:04d}-{month:02d}"

        for fallback_period in _iter_prior_months(anchor_period, fallback_months):
            fallback_results = self._fetch_dataset(
                base_url=self.generator_capacity_url,
                data_fields=data_fields,
                start=fallback_period,
                end=fallback_period,
                frequency="monthly",
                facets=facets,
                sort=sort,
                length=length,
            )
            if fallback_results:
                self.log.info(
                    "EIA capacity returned empty for %s-%s; using fallback month %s",
                    start_period,
                    end_period,
                    fallback_period,
                )
                return fallback_results

        return results

    def fetch_rto_solar_generation(
        self,
        respondents: List[str],
        start_period: Optional[str] = None,
        end_period: Optional[str] = None,
        length: int = 5000,
    ) -> List[Dict]:
        """
        Retrieve hourly solar generation (MWh) by balancing authority (respondent).
        """
        facets = {"fueltype": ["SUN"], "respondent": respondents}
        return self._fetch_dataset(
            base_url=self.rto_generation_url,
            data_fields=["value"],
            start=start_period,
            end=end_period,
            frequency="hourly",
            facets=facets,
            length=length,
        )
