import os
from typing import Iterable

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from etl.utils.db_connection import get_connection as get_source_connection

TABLES_TO_SYNC = (
    ("public", "bronze_climate_raw"),
    ("public", "silver_climate_hourly_history"),
    ("public", "silver_climate_hourly"),
    ("public", "gold_daily_summary_history"),
    ("public", "gold_daily_summary"),
)


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is required.")
    return value


def get_target_connection():
    timezone = os.getenv("NEON_TIMEZONE") or _require_env("TIMEZONE")
    sslmode = os.getenv("NEON_SSLMODE", "require")
    return psycopg2.connect(
        host=_require_env("NEON_DB_HOST"),
        port=int(_require_env("NEON_DB_PORT")),
        database=_require_env("NEON_DB_NAME"),
        user=_require_env("NEON_DB_USER"),
        password=_require_env("NEON_DB_PASS"),
        sslmode=sslmode,
        options=f"-c timezone={timezone}",
    )


def _fetch_columns(conn, schema_name: str, table_name: str) -> list[tuple[str, str, bool]]:
    query = """
        SELECT
            a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
            a.attnotnull AS not_null
        FROM pg_attribute AS a
        INNER JOIN pg_class AS c
            ON a.attrelid = c.oid
        INNER JOIN pg_namespace AS n
            ON c.relnamespace = n.oid
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        rows = cur.fetchall()
    if not rows:
        raise ValueError(f"Table or view not found: {schema_name}.{table_name}")
    return rows


def _fetch_primary_key_columns(conn, schema_name: str, table_name: str) -> list[str]:
    query = """
        SELECT a.attname
        FROM pg_index AS i
        INNER JOIN pg_class AS c
            ON i.indrelid = c.oid
        INNER JOIN pg_namespace AS n
            ON c.relnamespace = n.oid
        INNER JOIN pg_attribute AS a
            ON a.attrelid = c.oid
           AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = %s
          AND c.relname = %s
          AND i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum)
    """
    with conn.cursor() as cur:
        cur.execute(query, (schema_name, table_name))
        return [row[0] for row in cur.fetchall()]


def _create_shadow_table(target_conn, schema_name: str, table_name: str, columns, primary_key_columns: Iterable[str]) -> str:
    shadow_name = f"{table_name}__sync_tmp"
    column_defs = []
    for column_name, column_type, not_null in columns:
        column_def = f'"{column_name}" {column_type}'
        if not_null:
            column_def += " NOT NULL"
        column_defs.append(column_def)

    primary_key_columns = list(primary_key_columns)
    if primary_key_columns:
        quoted_pk = ", ".join(f'"{column}"' for column in primary_key_columns)
        column_defs.append(f"PRIMARY KEY ({quoted_pk})")

    create_sql = sql.SQL(
        """
        CREATE SCHEMA IF NOT EXISTS {schema};
        DROP TABLE IF EXISTS {schema}.{shadow};
        CREATE TABLE {schema}.{shadow} (
            {columns}
        )
        """
    ).format(
        schema=sql.Identifier(schema_name),
        shadow=sql.Identifier(shadow_name),
        columns=sql.SQL(", ").join(sql.SQL(part) for part in column_defs),
    )

    with target_conn:
        with target_conn.cursor() as cur:
            cur.execute(create_sql)

    return shadow_name


def _copy_rows(source_conn, target_conn, schema_name: str, source_table_name: str, target_table_name: str, columns) -> int:
    column_names = [column_name for column_name, _, _ in columns]
    total_rows = 0

    select_query = sql.SQL("SELECT * FROM {schema}.{table}").format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(source_table_name),
    )
    insert_query = sql.SQL("INSERT INTO {schema}.{table} ({columns}) VALUES %s").format(
        schema=sql.Identifier(schema_name),
        table=sql.Identifier(target_table_name),
        columns=sql.SQL(", ").join(sql.Identifier(column_name) for column_name in column_names),
    )

    source_cursor_name = f"sync_{schema_name}_{source_table_name}"
    with source_conn.cursor(name=source_cursor_name) as source_cur:
        source_cur.itersize = 1000
        source_cur.execute(select_query)
        while True:
            rows = source_cur.fetchmany(1000)
            if not rows:
                break
            with target_conn:
                with target_conn.cursor() as target_cur:
                    execute_values(target_cur, insert_query.as_string(target_conn), rows, page_size=1000)
            total_rows += len(rows)

    return total_rows


def _swap_tables(target_conn, schema_name: str, table_name: str, shadow_name: str) -> None:
    statements = [
        sql.SQL("DROP VIEW IF EXISTS {schema}.{table} CASCADE").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        ),
        sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {schema}.{table} CASCADE").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        ),
        sql.SQL("DROP TABLE IF EXISTS {schema}.{table} CASCADE").format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
        ),
        sql.SQL("ALTER TABLE {schema}.{shadow} RENAME TO {table}").format(
            schema=sql.Identifier(schema_name),
            shadow=sql.Identifier(shadow_name),
            table=sql.Identifier(table_name),
        ),
    ]
    with target_conn:
        with target_conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)


def sync_to_neon() -> None:
    print("Sincronizando dados do Postgres local para o Neon...")
    source_conn = get_source_connection()
    target_conn = get_target_connection()

    try:
        for schema_name, table_name in TABLES_TO_SYNC:
            print(f"- Sincronizando {schema_name}.{table_name}")
            columns = _fetch_columns(source_conn, schema_name, table_name)
            primary_key_columns = _fetch_primary_key_columns(source_conn, schema_name, table_name)
            shadow_name = _create_shadow_table(target_conn, schema_name, table_name, columns, primary_key_columns)
            copied_rows = _copy_rows(
                source_conn=source_conn,
                target_conn=target_conn,
                schema_name=schema_name,
                source_table_name=table_name,
                target_table_name=shadow_name,
                columns=columns,
            )
            _swap_tables(target_conn, schema_name, table_name, shadow_name)
            print(f"  {copied_rows} linhas copiadas")
    finally:
        source_conn.close()
        target_conn.close()

    print("NEON: sincronizacao concluida com sucesso.")


if __name__ == "__main__":
    sync_to_neon()
