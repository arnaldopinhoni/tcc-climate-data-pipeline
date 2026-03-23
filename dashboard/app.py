import os
import sys
from datetime import date, timedelta

# Garante que a raiz do repo esteja no path (necessário no Streamlit Cloud)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

from dashboard.charts import plot_et0, plot_hourly, plot_precipitation, plot_temperature
from dashboard.queries import CITY_NAMES, last_ingestion, load_gold, load_hourly

st.set_page_config(
    page_title="Monitoramento Climático — Interior Paulista",
    page_icon="🌱",
    layout="wide",
)

st.title("Monitoramento Climático — Interior Paulista")
st.caption(f"Última ingestão: {last_ingestion()}")

# --- Sidebar ---
with st.sidebar:
    st.header("Filtros")

    all_cities = list(CITY_NAMES.keys())
    cidades_selecionadas = st.multiselect(
        "Cidades",
        options=all_cities,
        default=all_cities,
        format_func=lambda k: CITY_NAMES[k],
    )

    hoje = date.today()
    data_inicio, data_fim = st.date_input(
        "Período",
        value=(hoje - timedelta(days=6), hoje),
        min_value=date(2020, 1, 1),
        max_value=hoje,
    )

    if st.button("Atualizar dados"):
        st.cache_data.clear()
        st.rerun()

if not cidades_selecionadas:
    st.warning("Selecione ao menos uma cidade.")
    st.stop()

df = load_gold(data_inicio, data_fim, tuple(cidades_selecionadas))

if df.empty:
    st.info("Nenhum dado disponível para o período e cidades selecionados.")
    st.stop()

# --- KPI Cards ---
hoje_df = df[df["day"] == df["day"].max()]
periodo_df = df

cols = st.columns(len(cidades_selecionadas))
for col, city_key in zip(cols, cidades_selecionadas):
    city_label = CITY_NAMES[city_key]
    city_hoje = hoje_df[hoje_df["city"] == city_key]
    city_periodo = periodo_df[periodo_df["city"] == city_key]

    avg_temp = city_hoje["avg_temp"].values[0] if not city_hoje.empty else None
    max_temp = city_hoje["max_temp"].values[0] if not city_hoje.empty else None
    chuva_7d = city_periodo["total_precipitation"].sum() if not city_periodo.empty else None
    et0_hoje = city_hoje["total_et0_fao_evapotranspiration"].values[0] if not city_hoje.empty else None

    with col:
        st.markdown(f"**{city_label}**")
        st.metric("Temp. média hoje", f"{avg_temp:.1f} °C" if avg_temp is not None else "—")
        st.metric("Temp. máx hoje", f"{max_temp:.1f} °C" if max_temp is not None else "—")
        st.metric("Chuva (período)", f"{chuva_7d:.1f} mm" if chuva_7d is not None else "—")
        st.metric("ET₀ hoje", f"{et0_hoje:.1f} mm/dia" if et0_hoje is not None else "—")

st.divider()

# --- Gráficos diários ---
st.subheader("Temperatura (°C)")
st.plotly_chart(plot_temperature(df), use_container_width=True)

st.subheader("Precipitação (mm)")
st.plotly_chart(plot_precipitation(df), use_container_width=True)

st.subheader("ET₀ FAO-56 (mm/dia)")
st.plotly_chart(plot_et0(df), use_container_width=True)
st.caption(
    "ET₀ representa a evapotranspiração de referência FAO-56. "
    "Valores acima de 5 mm/dia indicam alta demanda hídrica."
)

st.divider()

# --- Dados horários ---
with st.expander("Dados Horários"):
    col_sel, col_dia = st.columns(2)
    with col_sel:
        cidade_hora_key = st.selectbox(
            "Cidade",
            options=cidades_selecionadas,
            format_func=lambda k: CITY_NAMES[k],
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
        st.plotly_chart(plot_hourly(df_h), use_container_width=True)
