"""
EPA Envirofacts hook using the public GraphQL endpoint.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional
from xml.etree import ElementTree

import requests
from airflow.hooks.base import BaseHook
from typing import List, Optional

# Query aligned with the provided GraphQL sample for CO2 emissions.
EPA_DEFAULT_QUERY = """
query batchQuery($offset: Integer, $limit: Integer, $orderBy: OrderBy!) {
  ghg__rlps_ghg_emitter_gas(
    offset: $offset
    limit: $limit
    orderBy: $orderBy
    where: { gas_code: { equals: "CO2" } }
  ) {
    co2e_emission
    gas_code
    gas_name
    year
    state
    state_name
  }
}
"""


class EPAHook(BaseHook):
    """
    Fetches greenhouse gas emissions from the EPA GraphQL endpoint.
    """

    conn_name_attr = "epa_conn_id"
    default_conn_name = "epa_default"

    def __init__(
        self,
        endpoint: Optional[str] = None,
        query: str = EPA_DEFAULT_QUERY,
        request_timeout: int = 60,
    ) -> None:
        super().__init__()
        self.endpoint = endpoint or os.getenv(
            "EPA_GRAPHQL_ENDPOINT", "https://data.epa.gov/dmapservice/query/graphql"
        )
        self.query = query
        self.request_timeout = request_timeout

    def get_latest_year(self) -> int:
        """
        Descobre o ano mais recente com dados disponíveis na EPA.
        
        Returns:
            int: Ano mais recente disponível
        """
        try:
            # Buscar amostra pequena ordenada por ano DESC
            records = self.fetch_emissions(
                limit=100, 
                offset=0, 
                order_by={"year": "DESC"}
            )
            
            if records:
                # Extrair anos únicos
                years = {int(r.get("year")) for r in records if r.get("year")}
                if years:
                    latest = max(years)
                    self.log.info(f"Latest EPA year available: {latest}")
                    return latest
        
        except Exception as e:
            self.log.warning(f"Failed to fetch latest EPA year: {e}")
        
        # Fallback para estimar baseado no ano atual
        from datetime import datetime
        fallback = datetime.now().year - 2
        self.log.warning(f"Using fallback EPA year: {fallback}")
        return fallback

    def fetch_emissions(
        self,
        state_code: Optional[str] = None,
        years: Optional[List[str]] = None,
        limit: int = 5000,
        offset: int = 0,
        order_by: Optional[Dict[str, str]] = None,
    ) -> List[Dict]:
        """
        Pull emissions data using the GraphQL endpoint and the provided order_by.
        """
        variables: Dict[str, object] = {
            "offset": offset,
            "limit": limit,
            "orderBy": order_by or {"year": "DESC"},
        }
        # Optional filters for future extension.
        if years:
            variables["year"] = [str(y) for y in years]
        if state_code:
            variables["state_code"] = state_code

        payload = {
            "operationName": "batchQuery",
            "query": self.query,
            "variables": variables,
        }

        resp = requests.post(
            self.endpoint, json=payload, timeout=self.request_timeout
        )
        if resp.status_code >= 400:
            msg = resp.text
            raise requests.HTTPError(
                f"EPA API returned {resp.status_code}: {msg[:500]}", response=resp
            )
        records = self._extract_records(resp)
        if years:
            allowed = {str(y) for y in years}
            records = [row for row in records if str(row.get("year")) in allowed]
        return records

    def _extract_records(self, response: requests.Response) -> List[Dict]:
        """
        Try GraphQL JSON first, then XML as a fallback.
        """
        try:
            payload = response.json()
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            records: List[Dict] = []
            if "ghg__rlps_ghg_emitter_gas" in data:
                records = data["ghg__rlps_ghg_emitter_gas"] or []
            elif "ghg__rlps_fac_year_agg__aggregate" in data:
                records = data["ghg__rlps_fac_year_agg__aggregate"] or []
            elif "ghg__rlps_fac_year_agg" in data:
                records = data["ghg__rlps_fac_year_agg"] or []
            else:
                for value in data.values():
                    if isinstance(value, list):
                        records.extend(value)
            if records:
                return records
        except ValueError:
            # Not JSON, continue to XML parsing.
            pass

        try:
            root = ElementTree.fromstring(response.text)
        except ElementTree.ParseError as exc:
            raise ValueError(f"Unable to parse EPA response: {exc}") from exc

        records: List[Dict] = []
        for child in list(root):
            row: Dict[str, object] = {}
            for element in list(child):
                row[element.tag] = element.text
            if row:
                records.append(row)
        return records
