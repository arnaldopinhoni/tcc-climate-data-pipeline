import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

COLORS = {
    "Ribeirão Preto": "#e63946",
    "Piracicaba": "#2a9d8f",
    "Campinas": "#e9c46a",
    "São José do Rio Preto": "#457b9d",
    "Presidente Prudente": "#8338ec",
}


def _color(city_label: str) -> str:
    return COLORS.get(city_label, "#888888")


def plot_temperature(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city]
        color = _color(city)
        fig.add_trace(go.Scatter(
            x=city_df["day"], y=city_df["avg_temp"],
            name=f"{city} (média)", line=dict(color=color),
            mode="lines+markers",
        ))
        fig.add_trace(go.Scatter(
            x=city_df["day"], y=city_df["max_temp"],
            name=f"{city} (máx)", line=dict(color=color, dash="dot"),
            mode="lines+markers",
        ))
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="Temperatura (°C)",
        legend_title="Cidade",
        hovermode="x unified",
    )
    return fig


def plot_precipitation(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city]
        fig.add_trace(go.Bar(
            x=city_df["day"], y=city_df["total_precipitation"],
            name=city, marker_color=_color(city),
        ))
    fig.update_layout(
        barmode="group",
        xaxis_title="Data",
        yaxis_title="Precipitação (mm)",
        legend_title="Cidade",
        hovermode="x unified",
    )
    return fig


def plot_et0(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city]
        fig.add_trace(go.Bar(
            x=city_df["day"], y=city_df["total_et0_fao_evapotranspiration"],
            name=city, marker_color=_color(city),
        ))
    fig.add_hline(
        y=5, line_dash="dash", line_color="red",
        annotation_text="> 5 mm/dia = alta demanda hídrica",
        annotation_position="top left",
    )
    fig.update_layout(
        barmode="group",
        xaxis_title="Data",
        yaxis_title="ET₀ (mm/dia)",
        legend_title="Cidade",
        hovermode="x unified",
    )
    return fig


def plot_hourly(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city]
        color = _color(city)
        fig.add_trace(
            go.Scatter(
                x=city_df["record_time"], y=city_df["temperature_2m"],
                name=f"{city} — Temp (°C)", line=dict(color=color),
                mode="lines",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=city_df["record_time"], y=city_df["et0_fao_evapotranspiration"],
                name=f"{city} — ET₀ (mm/h)", line=dict(color=color, dash="dot"),
                mode="lines",
            ),
            secondary_y=True,
        )
    fig.update_yaxes(title_text="Temperatura (°C)", secondary_y=False)
    fig.update_yaxes(title_text="ET₀ (mm/h)", secondary_y=True)
    fig.update_layout(xaxis_title="Hora", hovermode="x unified")
    return fig
