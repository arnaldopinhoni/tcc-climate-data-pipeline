import os

import requests
from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is required.")
    return value


def get_open_meteo(lat: float, lon: float) -> dict:
    """
    Consulta a API Open-Meteo e retorna o JSON bruto.
    """
    base_url = _require_env("OPEN_METEO_BASE_URL")
    hourly_params = _require_env("OPEN_METEO_HOURLY_PARAMS")
    timezone = _require_env("OPEN_METEO_TIMEZONE")
    timeout_seconds = int(_require_env("OPEN_METEO_TIMEOUT_SECONDS"))

    response = requests.get(
        base_url,
        params={
            "latitude": lat,
            "longitude": lon,
            "hourly": hourly_params,
            "timezone": timezone,
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    return response.json()