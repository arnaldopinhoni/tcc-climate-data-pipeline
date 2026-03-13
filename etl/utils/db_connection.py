import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is required.")
    return value


def get_connection():
    return psycopg2.connect(
        host=_require_env("DB_HOST"),
        port=int(_require_env("DB_PORT")),
        database=_require_env("DB_NAME"),
        user=_require_env("DB_USER"),
        password=_require_env("DB_PASS"),
        options=f"-c timezone={_require_env('TIMEZONE')}",
    )
