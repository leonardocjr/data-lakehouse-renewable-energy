"""
Hook for NREL Developer API solar resource endpoint.
"""

from __future__ import annotations

import os
from typing import Optional

import requests
from airflow.hooks.base import BaseHook


class NRELHook(BaseHook):
    """
    Fetch solar resource (GHI) data from the NREL Developer API.
    """

    conn_name_attr = "nrel_conn_id"
    default_conn_name = "nrel_default"
    base_url = "https://developer.nrel.gov/api/solar/solar_resource/v1.json"

    def __init__(
        self,
        api_key: Optional[str] = None,
        nrel_conn_id: Optional[str] = None,
        request_timeout: int = 60,
    ) -> None:
        super().__init__()
        self.api_key = api_key or self._resolve_api_key(nrel_conn_id)
        self.request_timeout = request_timeout

    def _resolve_api_key(self, conn_id: Optional[str]) -> Optional[str]:
        if conn_id:
            conn = self.get_connection(conn_id)
            if conn.password:
                return conn.password
        return os.getenv("NREL_API_KEY")

    def fetch_solar_resource(self, latitude: float, longitude: float) -> dict:
        """
        Retrieve solar resource metrics (including average GHI) for a lat/lon pair.
        """
        if not self.api_key:
            raise ValueError("NREL API key not configured (set NREL_API_KEY or a conn).")

        params = {"api_key": self.api_key, "lat": latitude, "lon": longitude}
        resp = requests.get(self.base_url, params=params, timeout=self.request_timeout)
        resp.raise_for_status()
        return resp.json()
