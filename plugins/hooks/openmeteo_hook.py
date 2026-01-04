import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.sdk.bases.hook import BaseHook


class OpenMeteoHook(BaseHook):
    """Hook para interagir com a API Open-Meteo"""
    
    def __init__(self):
        super().__init__()
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.session = self._create_session_with_retries()
        self.logger = logging.getLogger(__name__)
    
    def _create_session_with_retries(self):
        """Cria sessão HTTP com retry automático"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session
    
    def fetch_hourly_weather(
        self, 
        latitude: float, 
        longitude: float, 
        start_date: str, 
        end_date: str
    ) -> dict:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "hourly": "temperature_2m,direct_radiation,cloudcover",
            "timezone": "UTC",
            "start_date": start_date,
            "end_date": end_date,
        }
        
        try:
            print(f"Fetching Open-Meteo data for lat={latitude}, lon={longitude}")
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.SSLError as e:
            print(f"SSL Error, retrying without verification: {e}")
            response = self.session.get(
                self.base_url, 
                params=params, 
                timeout=30, 
                verify=False
            )
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch Open-Meteo data: {e}")
            raise