import json
import os
from typing import Dict

from dotenv import load_dotenv

from etl.utils.api_client import get_open_meteo
from etl.utils.db_connection import get_connection

load_dotenv()


CityConfig = Dict[str, Dict[str, float]]


def _load_cities() -> CityConfig:
    raw_cities = os.getenv("OPEN_METEO_CITIES_JSON")
    if not raw_cities:
        raise ValueError("Environment variable 'OPEN_METEO_CITIES_JSON' is required.")

    try:
        parsed_cities = json.loads(raw_cities)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON in 'OPEN_METEO_CITIES_JSON'.") from exc

    if not isinstance(parsed_cities, dict) or not parsed_cities:
        raise ValueError("'OPEN_METEO_CITIES_JSON' must be a non-empty object.")

    normalized_cities: CityConfig = {}
    for city, coords in parsed_cities.items():
        if not isinstance(coords, dict) or "lat" not in coords or "lon" not in coords:
            raise ValueError(
                f"City '{city}' must include numeric 'lat' and 'lon' fields."
            )

        normalized_cities[city] = {
            "lat": float(coords["lat"]),
            "lon": float(coords["lon"]),
        }

    return normalized_cities


def ingest_to_bronze() -> None:
    print("Coletando dados da API Open-Meteo para cidades configuradas...")

    cities = _load_cities()
    conn = get_connection()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO bronze_climate_raw (city, raw_json)
        VALUES (%s, %s)
    """

    try:
        for city, coords in cities.items():
            print(f"- Coletando cidade: {city}")
            raw_json = get_open_meteo(lat=coords["lat"], lon=coords["lon"])
            cur.execute(insert_sql, (city, json.dumps(raw_json)))

        conn.commit()
    finally:
        cur.close()
        conn.close()

    print("BRONZE: ingestao multi-cidade concluida com sucesso.")


if __name__ == "__main__":
    ingest_to_bronze()