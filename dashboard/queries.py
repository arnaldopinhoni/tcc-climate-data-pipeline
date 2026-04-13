import pandas as pd
import streamlit as st

from dashboard.db import get_conn

CITY_NAMES = {
    "ribeirao_preto": "Ribeirão Preto",
    "piracicaba": "Piracicaba",
    "campinas": "Campinas",
    "sao_jose_do_rio_preto": "São José do Rio Preto",
    "presidente_prudente": "Presidente Prudente",
}


@st.cache_data(ttl=60)
def load_gold(start_date, end_date, cities: tuple) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ON (city, day)
               city, day, avg_temp, max_temp,
               total_precipitation, total_et0_fao_evapotranspiration,
               avg_dew_point_2m, avg_shortwave_radiation,
               avg_wind_speed_10m, avg_vapour_pressure_deficit
        FROM gold_daily_summary_history
        WHERE day BETWEEN %(start)s AND %(end)s
          AND city = ANY(%(cities)s)
        ORDER BY city, day, ingestion_time DESC, bronze_record_id DESC
    """
    conn = get_conn()
    df = pd.read_sql(sql, conn, params={"start": start_date, "end": end_date, "cities": list(cities)})
    df["city_label"] = df["city"].map(CITY_NAMES).fillna(df["city"])
    df = df.sort_values(["day", "city"]).reset_index(drop=True)
    return df


@st.cache_data(ttl=60)
def load_hourly(day, cities: tuple) -> pd.DataFrame:
    sql = """
        SELECT DISTINCT ON (city, record_time)
               record_time, city, temperature_2m,
               et0_fao_evapotranspiration, precipitation, relative_humidity_2m
        FROM silver_climate_hourly_history
        WHERE date(record_time) = %(day)s AND city = ANY(%(cities)s)
        ORDER BY city, record_time, ingestion_time DESC, bronze_record_id DESC
    """
    conn = get_conn()
    df = pd.read_sql(sql, conn, params={"day": day, "cities": list(cities)})
    df["city_label"] = df["city"].map(CITY_NAMES).fillna(df["city"])
    return df


@st.cache_data(ttl=60)
def last_ingestion() -> str:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(ingestion_time) FROM gold_daily_summary")
        val = cur.fetchone()[0]
    return val.strftime("%d/%m/%Y %H:%M") if val else "—"
