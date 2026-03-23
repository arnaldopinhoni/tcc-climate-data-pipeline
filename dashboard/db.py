import os

import psycopg2
import streamlit as st
from dotenv import load_dotenv


def _get_params() -> dict:
    try:
        s = st.secrets["postgres"]
        return dict(
            host=s["host"],
            port=s["port"],
            database=s["database"],
            user=s["user"],
            password=s["password"],
            sslmode="require",
            options=f"-c timezone={s.get('timezone', 'America/Sao_Paulo')}",
        )
    except Exception:
        load_dotenv()
        return dict(
            host=os.environ["DB_HOST"],
            port=int(os.environ["DB_PORT"]),
            database=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASS"],
            options=f"-c timezone={os.environ['TIMEZONE']}",
        )


@st.cache_resource
def get_conn():
    return psycopg2.connect(**_get_params())
