"""
Valida as previsoes coletadas contra a reanalise ERA5.

A reanalise ERA5 (ECMWF) assimila observacoes de estacoes de superficie, radiossondas,
radares e satelites em um modelo numerico, produzindo uma grade continua de valores
para o passado. Nao e medicao direta de estacao: e a melhor estimativa do estado da
atmosfera dado tudo o que foi observado. Usar ERA5 como referencia e pratica corrente
em verificacao de previsao quando nao ha rede de estacoes suficientemente densa, mas a
diferenca em relacao a medicao instrumental deve ser declarada.

Metodo
------
1. Monta a base deduplicada: uma previsao por cidade e por data-alvo, a da rodada mais
   recente DENTRO da janela de coleta. O recorte da janela vem antes da deduplicacao.
2. Busca o ERA5 diario para as mesmas coordenadas e datas, via archive-api.open-meteo.com.
3. Calcula vies, erro absoluto medio (MAE) e raiz do erro quadratico medio (RMSE) para
   temperatura media diaria e precipitacao diaria, por municipio e no conjunto.

Uso
---
    uv run python -m etl.validation.validate_against_era5
    uv run python -m etl.validation.validate_against_era5 --csv validacao_era5.csv
    uv run python -m etl.validation.validate_against_era5 \
        --ingest-start 2026-03-14 --ingest-end 2026-04-01
"""

import argparse
import json
import math
import os

import requests
from dotenv import load_dotenv

from etl.utils.db_connection import get_connection

load_dotenv()

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Recorta a janela de coleta e so entao escolhe, para cada data-alvo, a rodada mais
# recente. Inverter essa ordem selecionaria rodadas posteriores ao fim da coleta.
DEDUP_SQL = """
    select distinct on (city, day)
        city,
        day,
        (day - ingestion_time::date) as lead_days,
        avg_temp,
        total_precipitation
    from gold_daily_summary_history
    where ingestion_time::date between %(inicio)s and %(fim)s
    order by city, day, ingestion_time desc, bronze_record_id desc
"""


def _load_cities() -> dict:
    raw = os.getenv("OPEN_METEO_CITIES_JSON")
    if not raw:
        raise ValueError("Environment variable 'OPEN_METEO_CITIES_JSON' is required.")
    return json.loads(raw)


def load_forecasts(inicio: str, fim: str) -> dict:
    """Previsoes deduplicadas por cidade: {city: {day: (temp, precip, lead)}}."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(DEDUP_SQL, {"inicio": inicio, "fim": fim})
            rows = cur.fetchall()
    finally:
        conn.close()

    forecasts: dict = {}
    for city, day, lead, temp, precip in rows:
        forecasts.setdefault(city, {})[day.isoformat()] = (
            float(temp),
            float(precip),
            int(lead),
        )
    return forecasts


def load_era5(lat: float, lon: float, inicio: str, fim: str, timeout: int) -> dict:
    """ERA5 diario: {day: (temp_media, precip_total)}."""
    response = requests.get(
        ARCHIVE_URL,
        params={
            "latitude": lat,
            "longitude": lon,
            "start_date": inicio,
            "end_date": fim,
            "daily": "temperature_2m_mean,precipitation_sum",
            "timezone": os.getenv("OPEN_METEO_TIMEZONE", "America/Sao_Paulo"),
        },
        timeout=timeout,
    )
    response.raise_for_status()
    daily = response.json()["daily"]
    return {
        day: (temp, precip)
        for day, temp, precip in zip(
            daily["time"], daily["temperature_2m_mean"], daily["precipitation_sum"]
        )
    }


def _scores(errors: list) -> tuple:
    n = len(errors)
    vies = sum(errors) / n
    mae = sum(abs(e) for e in errors) / n
    rmse = math.sqrt(sum(e * e for e in errors) / n)
    return vies, mae, rmse


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingest-start", default="2026-03-14")
    parser.add_argument("--ingest-end", default="2026-04-01")
    parser.add_argument("--csv", default=None, help="Grava o resultado por municipio.")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    cities = _load_cities()
    forecasts = load_forecasts(args.ingest_start, args.ingest_end)
    if not forecasts:
        raise SystemExit(
            f"Nenhuma previsao encontrada na janela {args.ingest_start} a {args.ingest_end}."
        )

    linhas = []
    erros_temp_geral: list = []
    erros_precip_geral: list = []

    for city, coords in sorted(cities.items()):
        previsto = forecasts.get(city)
        if not previsto:
            print(f"- {city}: sem previsoes na janela, pulando")
            continue

        dias = sorted(previsto)
        observado = load_era5(
            coords["lat"], coords["lon"], dias[0], dias[-1], args.timeout
        )

        erros_temp, erros_precip = [], []
        soma_prevista = soma_observada = 0.0
        for dia in dias:
            obs = observado.get(dia)
            if obs is None or obs[0] is None or obs[1] is None:
                continue
            temp, precip, _lead = previsto[dia]
            erros_temp.append(temp - obs[0])
            erros_precip.append(precip - obs[1])
            soma_prevista += precip
            soma_observada += obs[1]

        if not erros_temp:
            print(f"- {city}: sem sobreposicao com o ERA5, pulando")
            continue

        erros_temp_geral.extend(erros_temp)
        erros_precip_geral.extend(erros_precip)

        vies_t, mae_t, rmse_t = _scores(erros_temp)
        vies_p, mae_p, rmse_p = _scores(erros_precip)
        leads = [previsto[d][2] for d in dias]

        linhas.append(
            {
                "municipio": city,
                "dias": len(erros_temp),
                "antecedencia_media": round(sum(leads) / len(leads), 2),
                "vies_temp_c": round(vies_t, 2),
                "mae_temp_c": round(mae_t, 2),
                "rmse_temp_c": round(rmse_t, 2),
                "vies_precip_mm_dia": round(vies_p, 2),
                "mae_precip_mm_dia": round(mae_p, 2),
                "rmse_precip_mm_dia": round(rmse_p, 2),
                "precip_prevista_mm": round(soma_prevista, 1),
                "precip_era5_mm": round(soma_observada, 1),
            }
        )

    cabecalho = (
        f"{'municipio':<24}{'n':>4}{'viesT':>8}{'MAE_T':>8}{'RMSE_T':>8}"
        f"{'viesP':>8}{'MAE_P':>8}{'RMSE_P':>8}{'P_prev':>9}{'P_ERA5':>9}"
    )
    print()
    print(f"Janela de coleta: {args.ingest_start} a {args.ingest_end}")
    print("Referencia: reanalise ERA5 (ECMWF) via Open-Meteo archive-api")
    print()
    print(cabecalho)
    print("-" * len(cabecalho))
    for linha in linhas:
        print(
            f"{linha['municipio']:<24}{linha['dias']:>4}"
            f"{linha['vies_temp_c']:>8.2f}{linha['mae_temp_c']:>8.2f}{linha['rmse_temp_c']:>8.2f}"
            f"{linha['vies_precip_mm_dia']:>8.2f}{linha['mae_precip_mm_dia']:>8.2f}"
            f"{linha['rmse_precip_mm_dia']:>8.2f}"
            f"{linha['precip_prevista_mm']:>9.1f}{linha['precip_era5_mm']:>9.1f}"
        )

    if erros_temp_geral:
        vies_t, mae_t, rmse_t = _scores(erros_temp_geral)
        vies_p, mae_p, rmse_p = _scores(erros_precip_geral)
        print("-" * len(cabecalho))
        print(
            f"{'CONJUNTO':<24}{len(erros_temp_geral):>4}"
            f"{vies_t:>8.2f}{mae_t:>8.2f}{rmse_t:>8.2f}"
            f"{vies_p:>8.2f}{mae_p:>8.2f}{rmse_p:>8.2f}"
        )

    if args.csv and linhas:
        import csv

        with open(args.csv, "w", newline="", encoding="utf-8") as arquivo:
            writer = csv.DictWriter(arquivo, fieldnames=list(linhas[0]))
            writer.writeheader()
            writer.writerows(linhas)
        print(f"\nGravado em {args.csv}")


if __name__ == "__main__":
    main()
