import os
import sys
from datetime import date, timedelta

import pandas as pd
import streamlit as st

# Garante que a raiz do repo esteja no path (necessário no Streamlit Cloud)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard.charts import plot_city_comparison, plot_daily_table, plot_et0, plot_hourly, plot_precipitation, plot_temperature
from dashboard.queries import CITY_NAMES, last_ingestion, load_gold, load_hourly

st.set_page_config(
    page_title="Painel Climático do Interior Paulista",
    page_icon="🌦️",
    layout="wide",
)

st.markdown(
    """
    <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(212, 154, 40, 0.18), transparent 28%),
                radial-gradient(circle at top right, rgba(47, 111, 109, 0.18), transparent 25%),
                linear-gradient(180deg, #f7f4ec 0%, #f2ede3 100%);
            color: #3e2d23;
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #23403f 0%, #2f6f6d 100%);
        }
        [data-testid="stSidebar"] * {
            color: #f8f5ef;
        }
        .hero {
            padding: 1.5rem 1.6rem;
            border: 1px solid rgba(62, 45, 35, 0.08);
            border-radius: 22px;
            background: linear-gradient(135deg, rgba(255, 253, 248, 0.96), rgba(245, 236, 218, 0.86));
            box-shadow: 0 16px 45px rgba(62, 45, 35, 0.08);
            margin-bottom: 1rem;
        }
        .hero h1 {
            font-size: 2.25rem;
            margin: 0 0 0.4rem 0;
            color: #2e241d;
        }
        .hero p {
            margin: 0;
            color: #5d4a3d;
            font-size: 1rem;
        }
        .section-note {
            padding: 0.9rem 1rem;
            border-radius: 16px;
            background: rgba(255, 253, 248, 0.9);
            border: 1px solid rgba(62, 45, 35, 0.08);
            color: #5d4a3d;
        }
        div[data-testid="metric-container"] {
            border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem;
            background: rgba(255, 253, 248, 0.92);
            border: 1px solid rgba(62, 45, 35, 0.08);
            box-shadow: 0 10px 30px rgba(62, 45, 35, 0.06);
        }
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
        }
        .stTabs [data-baseweb="tab"] {
            background: rgba(255, 253, 248, 0.8);
            border-radius: 999px;
            padding: 0.6rem 1rem;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def format_number(value: float | None, suffix: str, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "—"
    return f"{value:.{digits}f} {suffix}".strip()


def format_delta(value: float | None, suffix: str, digits: int = 1) -> str:
    if value is None or pd.isna(value):
        return "sem base comparativa"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.{digits}f} {suffix}".strip()


def summarize_city(df: pd.DataFrame, city_key: str) -> dict:
    city_df = df[df["city"] == city_key].sort_values("day")
    latest = city_df.iloc[-1] if not city_df.empty else None
    previous = city_df.iloc[-2] if len(city_df) > 1 else None
    return {
        "avg_temp": latest["avg_temp"] if latest is not None else None,
        "avg_temp_delta": (latest["avg_temp"] - previous["avg_temp"]) if latest is not None and previous is not None else None,
        "max_temp": latest["max_temp"] if latest is not None else None,
        "precip_total": city_df["total_precipitation"].sum() if not city_df.empty else None,
        "et0_latest": latest["total_et0_fao_evapotranspiration"] if latest is not None else None,
    }


st.markdown(
    f"""
    <div class="hero">
        <h1>Painel Climático do Interior Paulista</h1>
        <p>Monitore temperatura, precipitação, ET₀ e condições horárias nas cidades acompanhadas pelo pipeline.</p>
        <p><strong>Última ingestão:</strong> {last_ingestion()}</p>
        <p>O dashboard revalida o cache automaticamente a cada 60 segundos.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Filtros")

    all_cities = list(CITY_NAMES.keys())
    cidades_selecionadas = st.multiselect(
        "Cidades",
        options=all_cities,
        default=all_cities,
        format_func=lambda key: CITY_NAMES[key],
    )

    hoje = date.today()
    data_inicio, data_fim = st.date_input(
        "Período",
        value=(hoje - timedelta(days=6), hoje),
        min_value=date(2020, 1, 1),
        max_value=hoje,
    )

    metrica_comparativa = st.selectbox(
        "Ranking comparativo",
        options=[
            ("avg_temp", "Temperatura média"),
            ("max_temp", "Temperatura máxima"),
            ("total_precipitation", "Precipitação acumulada"),
            ("total_et0_fao_evapotranspiration", "ET₀ acumulada"),
            ("avg_wind_speed_10m", "Velocidade média do vento"),
        ],
        format_func=lambda item: item[1],
    )

    if st.button("Atualizar dados", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

if not cidades_selecionadas:
    st.warning("Selecione ao menos uma cidade.")
    st.stop()

df = load_gold(data_inicio, data_fim, tuple(cidades_selecionadas))

if df.empty:
    st.info("Nenhum dado disponível para o período e cidades selecionados.")
    st.stop()

df = df.sort_values(["day", "city_label"]).copy()
latest_day = df["day"].max()
overview_df = df[df["day"] == latest_day]
city_summary = (
    df.groupby(["city", "city_label"], as_index=False)
    .agg(
        avg_temp=("avg_temp", "mean"),
        max_temp=("max_temp", "max"),
        total_precipitation=("total_precipitation", "sum"),
        total_et0_fao_evapotranspiration=("total_et0_fao_evapotranspiration", "sum"),
        avg_wind_speed_10m=("avg_wind_speed_10m", "mean"),
    )
)

metric_meta = {
    "avg_temp": ("Temperatura média no período", "°C"),
    "max_temp": ("Pico de temperatura no período", "°C"),
    "total_precipitation": ("Precipitação acumulada", " mm"),
    "total_et0_fao_evapotranspiration": ("ET₀ acumulada", " mm"),
    "avg_wind_speed_10m": ("Velocidade média do vento", " km/h"),
}

overall_cols = st.columns(4)
overall_cols[0].metric("Temperatura média do último dia", format_number(overview_df["avg_temp"].mean(), "°C"))
overall_cols[1].metric("Precipitação acumulada no período", format_number(df["total_precipitation"].sum(), "mm"))
overall_cols[2].metric("ET₀ média no último dia", format_number(overview_df["total_et0_fao_evapotranspiration"].mean(), "mm/dia"))
overall_cols[3].metric("Vento médio no período", format_number(df["avg_wind_speed_10m"].mean(), "km/h"))

st.markdown(
    f"""
    <div class="section-note">
        O recorte atual cobre <strong>{len(cidades_selecionadas)}</strong> cidades entre
        <strong>{data_inicio.strftime("%d/%m/%Y")}</strong> e <strong>{data_fim.strftime("%d/%m/%Y")}</strong>.
        O último dia disponível na seleção é <strong>{latest_day.strftime("%d/%m/%Y")}</strong>.
    </div>
    """,
    unsafe_allow_html=True,
)

tab_overview, tab_cities, tab_hourly = st.tabs(["Visão Geral", "Comparativo por Cidade", "Leitura Horária"])

with tab_overview:
    col_temp, col_rank = st.columns([1.6, 1])
    with col_temp:
        st.subheader("Evolução diária da temperatura")
        st.plotly_chart(plot_temperature(df), use_container_width=True)
    with col_rank:
        metric_column, metric_label = metrica_comparativa
        title, suffix = metric_meta[metric_column]
        st.subheader("Ranking do período")
        st.plotly_chart(
            plot_city_comparison(city_summary, metric_column=metric_column, metric_title=title, suffix=suffix),
            use_container_width=True,
        )

    col_rain, col_et0 = st.columns(2)
    with col_rain:
        st.subheader("Precipitação diária")
        st.plotly_chart(plot_precipitation(df), use_container_width=True)
    with col_et0:
        st.subheader("ET₀ diária")
        st.plotly_chart(plot_et0(df), use_container_width=True)

    st.caption(
        "ET₀ representa a evapotranspiração de referência FAO-56. "
        "Valores diários acima de 5 mm/dia indicam maior demanda hídrica."
    )

with tab_cities:
    cards = st.columns(len(cidades_selecionadas))
    for col, city_key in zip(cards, cidades_selecionadas):
        summary = summarize_city(df, city_key)
        with col:
            st.markdown(f"**{CITY_NAMES[city_key]}**")
            st.metric("Temp. média no último dia", format_number(summary["avg_temp"], "°C"), format_delta(summary["avg_temp_delta"], "°C"))
            st.metric("Temp. máxima no período", format_number(summary["max_temp"], "°C"))
            st.metric("Chuva acumulada", format_number(summary["precip_total"], "mm"))
            st.metric("ET₀ no último dia", format_number(summary["et0_latest"], "mm/dia"))

    metric_choice = st.radio(
        "Métrica para análise comparativa",
        options=[
            ("avg_temp", "Temperatura média"),
            ("total_precipitation", "Precipitação"),
            ("total_et0_fao_evapotranspiration", "ET₀"),
        ],
        horizontal=True,
        format_func=lambda item: item[1],
    )
    chosen_metric, chosen_label = metric_choice
    st.plotly_chart(
        plot_daily_table(df, metric_column=chosen_metric, metric_title=chosen_label),
        use_container_width=True,
    )

    table_df = city_summary.rename(
        columns={
            "city_label": "Cidade",
            "avg_temp": "Temp. média (°C)",
            "max_temp": "Temp. máxima (°C)",
            "total_precipitation": "Chuva acumulada (mm)",
            "total_et0_fao_evapotranspiration": "ET₀ acumulada (mm)",
            "avg_wind_speed_10m": "Vento médio (km/h)",
        }
    ).drop(columns=["city"])
    st.dataframe(
        table_df.style.format(
            {
                "Temp. média (°C)": "{:.1f}",
                "Temp. máxima (°C)": "{:.1f}",
                "Chuva acumulada (mm)": "{:.1f}",
                "ET₀ acumulada (mm)": "{:.1f}",
                "Vento médio (km/h)": "{:.1f}",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

with tab_hourly:
    col_sel, col_dia = st.columns(2)
    with col_sel:
        cidade_hora_key = st.selectbox(
            "Cidade",
            options=cidades_selecionadas,
            format_func=lambda key: CITY_NAMES[key],
            key="hourly_city",
        )
    with col_dia:
        dia_hora = st.date_input(
            "Dia",
            value=data_fim,
            min_value=data_inicio,
            max_value=data_fim,
            key="hourly_day",
        )

    df_h = load_hourly(dia_hora, (cidade_hora_key,))
    if df_h.empty:
        st.info("Sem dados horários para a seleção.")
    else:
        hourly_cols = st.columns(4)
        hourly_cols[0].metric("Temp. mínima", format_number(df_h["temperature_2m"].min(), "°C"))
        hourly_cols[1].metric("Temp. máxima", format_number(df_h["temperature_2m"].max(), "°C"))
        hourly_cols[2].metric("Chuva acumulada", format_number(df_h["precipitation"].sum(), "mm"))
        hourly_cols[3].metric("Umidade média", format_number(df_h["relative_humidity_2m"].mean(), "%"))

        st.plotly_chart(plot_hourly(df_h), use_container_width=True)
        st.dataframe(
            df_h.rename(
                columns={
                    "record_time": "Data/Hora",
                    "temperature_2m": "Temperatura (°C)",
                    "relative_humidity_2m": "Umidade relativa (%)",
                    "precipitation": "Precipitação (mm)",
                    "et0_fao_evapotranspiration": "ET₀ (mm/h)",
                }
            )[
                [
                    "Data/Hora",
                    "Temperatura (°C)",
                    "Umidade relativa (%)",
                    "Precipitação (mm)",
                    "ET₀ (mm/h)",
                ]
            ].style.format(
                {
                    "Temperatura (°C)": "{:.1f}",
                    "Umidade relativa (%)": "{:.0f}",
                    "Precipitação (mm)": "{:.2f}",
                    "ET₀ (mm/h)": "{:.2f}",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
