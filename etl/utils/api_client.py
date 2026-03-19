import os

import requests
from dotenv import load_dotenv

load_dotenv()


ET0_REQUIRED_HOURLY_PARAMS = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "dew_point_2m",
    "shortwave_radiation",
    "wind_speed_10m",
    "vapour_pressure_deficit",
    "et0_fao_evapotranspiration",
)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is required.")
    return value


def _build_hourly_params(raw_hourly_params: str) -> str:
    params = []
    seen = set()

    for param in [*raw_hourly_params.split(","), *ET0_REQUIRED_HOURLY_PARAMS]:
        normalized_param = param.strip()
        if not normalized_param or normalized_param in seen:
            continue

        seen.add(normalized_param)
        params.append(normalized_param)

    return ",".join(params)


def get_open_meteo(lat: float, lon: float) -> dict:
    """
    Consulta a API Open-Meteo e retorna o JSON bruto.
    """
    base_url = _require_env("OPEN_METEO_BASE_URL")
    hourly_params = _build_hourly_params(_require_env("OPEN_METEO_HOURLY_PARAMS"))
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
