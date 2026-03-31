import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

COLORS = {
    "Ribeirão Preto": "#C84C31",
    "Piracicaba": "#2F6F6D",
    "Campinas": "#D49A28",
    "São José do Rio Preto": "#2C5C88",
    "Presidente Prudente": "#7A3E65",
}

BG_COLOR = "#F7F4EC"
PANEL_COLOR = "#FFFDF8"
GRID_COLOR = "rgba(62, 45, 35, 0.10)"
TEXT_COLOR = "#3E2D23"


def _color(city_label: str) -> str:
    return COLORS.get(city_label, "#7F7F7F")


def _apply_layout(fig: go.Figure, *, xaxis_title: str, yaxis_title: str, hovermode: str = "x unified") -> go.Figure:
    fig.update_layout(
        paper_bgcolor=PANEL_COLOR,
        plot_bgcolor=PANEL_COLOR,
        margin=dict(l=16, r=16, t=24, b=16),
        font=dict(color=TEXT_COLOR),
        legend_title="Cidade",
        hovermode=hovermode,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID_COLOR)
    fig.update_yaxes(gridcolor=GRID_COLOR, zeroline=False, linecolor=GRID_COLOR)
    return fig


def plot_temperature(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city].sort_values("day")
        color = _color(city)
        fig.add_trace(
            go.Scatter(
                x=city_df["day"],
                y=city_df["max_temp"],
                name=f"{city} (máx)",
                line=dict(color=color, width=1.8, dash="dot"),
                mode="lines+markers",
                marker=dict(size=7),
                opacity=0.75,
            )
        )
        fig.add_trace(
            go.Scatter(
                x=city_df["day"],
                y=city_df["avg_temp"],
                name=f"{city} (média)",
                line=dict(color=color, width=3),
                mode="lines+markers",
                marker=dict(size=8),
            )
        )
    return _apply_layout(fig, xaxis_title="Data", yaxis_title="Temperatura (°C)")


def plot_precipitation(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city].sort_values("day")
        fig.add_trace(
            go.Bar(
                x=city_df["day"],
                y=city_df["total_precipitation"],
                name=city,
                marker_color=_color(city),
                opacity=0.88,
            )
        )
    fig.update_layout(barmode="group")
    return _apply_layout(fig, xaxis_title="Data", yaxis_title="Precipitação (mm)")


def plot_et0(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city].sort_values("day")
        fig.add_trace(
            go.Bar(
                x=city_df["day"],
                y=city_df["total_et0_fao_evapotranspiration"],
                name=city,
                marker_color=_color(city),
                opacity=0.88,
            )
        )
    fig.add_hline(
        y=5,
        line_dash="dash",
        line_color="#9C2F1C",
        annotation_text="Alta demanda hídrica",
        annotation_position="top left",
    )
    fig.update_layout(barmode="group")
    return _apply_layout(fig, xaxis_title="Data", yaxis_title="ET₀ (mm/dia)")


def plot_city_comparison(summary_df: pd.DataFrame, metric_column: str, metric_title: str, suffix: str) -> go.Figure:
    ranked = summary_df.sort_values(metric_column, ascending=False)
    fig = go.Figure(
        go.Bar(
            x=ranked[metric_column],
            y=ranked["city_label"],
            orientation="h",
            marker_color=[_color(city) for city in ranked["city_label"]],
            text=[f"{value:.1f}{suffix}" for value in ranked[metric_column]],
            textposition="outside",
        )
    )
    fig.update_layout(showlegend=False)
    return _apply_layout(fig, xaxis_title=metric_title, yaxis_title="Cidade", hovermode="y unified")


def plot_daily_table(df: pd.DataFrame, metric_column: str, metric_title: str) -> go.Figure:
    ordered = df.sort_values(["day", "city_label"])
    fig = go.Figure()
    for city in ordered["city_label"].unique():
        city_df = ordered[ordered["city_label"] == city]
        fig.add_trace(
            go.Scatter(
                x=city_df["day"],
                y=city_df[metric_column],
                mode="lines+markers",
                name=city,
                line=dict(color=_color(city), width=3),
                marker=dict(size=7),
            )
        )
    return _apply_layout(fig, xaxis_title="Data", yaxis_title=metric_title)


def plot_hourly(df: pd.DataFrame) -> go.Figure:
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{"secondary_y": True}], [{"secondary_y": False}], [{"secondary_y": True}]],
        subplot_titles=(
            "Temperatura e Umidade",
            "Precipitação horária",
            "ET₀ e Umidade relativa",
        ),
    )

    for city in df["city_label"].unique():
        city_df = df[df["city_label"] == city].sort_values("record_time")
        color = _color(city)
        fig.add_trace(
            go.Scatter(
                x=city_df["record_time"],
                y=city_df["temperature_2m"],
                name=f"{city} - Temp",
                line=dict(color=color, width=2.8),
                mode="lines",
            ),
            row=1,
            col=1,
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=city_df["record_time"],
                y=city_df["relative_humidity_2m"],
                name=f"{city} - Umidade",
                line=dict(color=color, width=1.8, dash="dot"),
                mode="lines",
                opacity=0.75,
            ),
            row=1,
            col=1,
            secondary_y=True,
        )
        fig.add_trace(
            go.Bar(
                x=city_df["record_time"],
                y=city_df["precipitation"],
                name=f"{city} - Chuva",
                marker_color=color,
                opacity=0.7,
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=city_df["record_time"],
                y=city_df["et0_fao_evapotranspiration"],
                name=f"{city} - ET₀",
                line=dict(color=color, width=2.8),
                mode="lines",
            ),
            row=3,
            col=1,
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=city_df["record_time"],
                y=city_df["relative_humidity_2m"],
                name=f"{city} - UR",
                line=dict(color=color, width=1.6, dash="dot"),
                mode="lines",
                opacity=0.45,
                showlegend=False,
            ),
            row=3,
            col=1,
            secondary_y=True,
        )

    fig.update_yaxes(title_text="Temperatura (°C)", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Umidade (%)", row=1, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Precipitação (mm)", row=2, col=1)
    fig.update_yaxes(title_text="ET₀ (mm/h)", row=3, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Umidade (%)", row=3, col=1, secondary_y=True)
    fig.update_layout(
        paper_bgcolor=PANEL_COLOR,
        plot_bgcolor=PANEL_COLOR,
        margin=dict(l=16, r=16, t=56, b=16),
        font=dict(color=TEXT_COLOR),
        hovermode="x unified",
        legend_title="Séries",
        height=880,
    )
    fig.update_xaxes(showgrid=False, linecolor=GRID_COLOR, title_text="Hora", row=3, col=1)
    fig.update_yaxes(gridcolor=GRID_COLOR, zeroline=False, linecolor=GRID_COLOR)
    return fig
