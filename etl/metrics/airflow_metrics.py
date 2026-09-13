"""
Metricas operacionais do pipeline, lidas da metadata do Apache Airflow.

Extrai taxa de sucesso e duracao por tarefa a partir das tabelas dag_run e
task_instance do banco de metadata do Airflow, que fica no mesmo servico PostgreSQL,
em base separada (AIRFLOW__DATABASE__SQL_ALCHEMY_CONN aponta para a base 'airflow').

Complementa dbt/analyses/metricas_operacionais.sql, que mede volume de payload e
latencia de gravacao a partir da propria camada bronze.

Uso
---
    uv run python -m etl.metrics.airflow_metrics
    uv run python -m etl.metrics.airflow_metrics --inicio 2026-03-14 --fim 2026-04-01
    uv run python -m etl.metrics.airflow_metrics --csv metricas_operacionais.csv
"""

import argparse
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DAG_ID = "full_pipeline_open_meteo"

SQL_POR_TAREFA = """
    select
        ti.task_id,
        count(*) as execucoes,
        count(*) filter (where ti.state = 'success') as sucessos,
        count(*) filter (where ti.state = 'failed') as falhas,
        count(*) filter (where ti.try_number > 1) as com_retentativa,
        round(avg(ti.duration)::numeric, 1) as duracao_media_s,
        round(min(ti.duration)::numeric, 1) as duracao_min_s,
        round(max(ti.duration)::numeric, 1) as duracao_max_s,
        round(
            percentile_cont(0.5) within group (order by ti.duration)::numeric, 1
        ) as duracao_mediana_s
    from task_instance ti
    where ti.dag_id = %(dag_id)s
      and ti.start_date::date between %(inicio)s and %(fim)s
    group by 1
    order by 1
"""

SQL_POR_EXECUCAO = """
    select
        count(*) as execucoes_dag,
        count(*) filter (where state = 'success') as sucessos,
        count(*) filter (where state = 'failed') as falhas,
        count(distinct start_date::date) as dias_com_execucao,
        min(start_date::date) as primeira,
        max(start_date::date) as ultima,
        round(avg(extract(epoch from (end_date - start_date)))::numeric, 1) as duracao_media_s
    from dag_run
    where dag_id = %(dag_id)s
      and start_date::date between %(inicio)s and %(fim)s
"""


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Environment variable '{name}' is required.")
    return value


def get_airflow_connection():
    return psycopg2.connect(
        host=_require_env("DB_HOST"),
        port=int(_require_env("DB_PORT")),
        database=os.getenv("AIRFLOW_DB_NAME", "airflow"),
        user=_require_env("DB_USER"),
        password=_require_env("DB_PASS"),
        options=f"-c timezone={_require_env('TIMEZONE')}",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inicio", default="2026-03-14")
    parser.add_argument("--fim", default="2026-04-01")
    parser.add_argument("--dag-id", default=DAG_ID)
    parser.add_argument("--csv", default=None)
    args = parser.parse_args()

    params = {"dag_id": args.dag_id, "inicio": args.inicio, "fim": args.fim}

    conn = get_airflow_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(SQL_POR_EXECUCAO, params)
            resumo = cur.fetchone()
            cur.execute(SQL_POR_TAREFA, params)
            colunas = [d[0] for d in cur.description]
            tarefas = cur.fetchall()
    finally:
        conn.close()

    print(f"\nDAG {args.dag_id} | janela {args.inicio} a {args.fim}\n")
    if resumo and resumo[0]:
        execucoes, sucessos, falhas, dias, primeira, ultima, duracao = resumo
        taxa = 100.0 * sucessos / execucoes
        print(f"  Execucoes da DAG      {execucoes}")
        print(f"  Sucessos              {sucessos} ({taxa:.1f}%)")
        print(f"  Falhas                {falhas}")
        print(f"  Dias com execucao     {dias}")
        print(f"  Primeira / ultima     {primeira} / {ultima}")
        print(f"  Duracao media da DAG  {duracao} s")
    else:
        print("  Nenhuma execucao encontrada na janela.")

    if tarefas:
        print()
        largura = [14, 11, 10, 8, 14, 14, 12, 12, 14]
        print("".join(c.ljust(w) for c, w in zip(colunas, largura)))
        print("-" * sum(largura))
        for linha in tarefas:
            print("".join(str(v).ljust(w) for v, w in zip(linha, largura)))

    if args.csv and tarefas:
        import csv

        with open(args.csv, "w", newline="", encoding="utf-8") as arquivo:
            writer = csv.writer(arquivo)
            writer.writerow(colunas)
            writer.writerows(tarefas)
        print(f"\nGravado em {args.csv}")


if __name__ == "__main__":
    main()
