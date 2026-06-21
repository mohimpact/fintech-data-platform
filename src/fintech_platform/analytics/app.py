"""Streamlit dashboard for Gold-layer merchant metrics."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from fintech_platform.analytics.data import load_dashboard_data


st.set_page_config(
    page_title="Fintech Data Platform | Gold Analytics",
    page_icon="FT",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    html, body, [class*="css"] { font-family: Inter, Arial, sans-serif; }
    .stApp { background: #0b1020; color: #e5e7eb; }
    div[data-testid="metric-container"] {
        background: #101827;
        border: 1px solid rgba(20, 184, 166, 0.22);
        border-radius: 8px;
        padding: 16px;
    }
    div[data-testid="metric-container"] label {
        color: #94a3b8 !important;
        font-size: 0.8rem !important;
        text-transform: uppercase;
    }
    div[data-testid="metric-container"] div[data-testid="metric-value"] {
        color: #5eead4 !important;
        font-weight: 700 !important;
    }
    [data-testid="stSidebar"] {
        background: #0f172a;
        border-right: 1px solid rgba(148, 163, 184, 0.18);
    }
    .status-live, .status-demo {
        display: inline-block;
        border-radius: 999px;
        padding: 4px 12px;
        font-size: 0.75rem;
        font-weight: 700;
    }
    .status-live {
        background: rgba(20, 184, 166, 0.16);
        border: 1px solid rgba(20, 184, 166, 0.45);
        color: #5eead4;
    }
    .status-demo {
        background: rgba(245, 158, 11, 0.14);
        border: 1px solid rgba(245, 158, 11, 0.4);
        color: #fbbf24;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_data(ttl=60)
def cached_dashboard_data(use_demo: bool) -> tuple[pd.DataFrame, bool]:
    return load_dashboard_data(use_demo=use_demo)


with st.sidebar:
    st.markdown("## Fintech Platform")
    st.markdown("---")
    data_source = st.radio("Data Source", ["Auto (MinIO -> Demo)", "Demo Mode"], index=0)
    st.markdown("---")
    st.markdown("### Architecture")
    st.markdown("Kafka -> Spark -> Iceberg -> Airflow -> Streamlit")
    st.markdown("---")
    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.title("Fintech Fraud Detection Analytics")
st.markdown("Gold-layer merchant metrics from the local lakehouse.")

with st.spinner("Loading Gold metrics..."):
    df, is_live = cached_dashboard_data(use_demo=data_source == "Demo Mode")

badge_class = "status-live" if is_live else "status-demo"
badge_text = "LIVE DATA" if is_live else "DEMO MODE"
st.markdown(f'<span class="{badge_class}">{badge_text}</span>', unsafe_allow_html=True)
st.markdown("---")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

total_volume = df["total_volume"].sum()
total_transactions = df["transaction_count"].sum()
average_transaction = df["average_transaction_size"].mean()
total_fraud = df["fraud_count"].sum() if "fraud_count" in df.columns else 0
fraud_rate = (total_fraud / total_transactions * 100) if total_transactions else 0

kpi_volume, kpi_transactions, kpi_average, kpi_fraud = st.columns(4)
kpi_volume.metric("Total Volume", f"${total_volume:,.0f}")
kpi_transactions.metric("Transactions", f"{total_transactions:,}")
kpi_average.metric("Avg Transaction", f"${average_transaction:,.2f}")
kpi_fraud.metric("Fraud Rate", f"{fraud_rate:.2f}%")

st.markdown("---")
volume_col, category_col = st.columns([2, 1])

with volume_col:
    st.subheader("Daily Transaction Volume")
    daily_volume = df.groupby("date")["total_volume"].sum().reset_index()
    volume_chart = px.area(
        daily_volume,
        x="date",
        y="total_volume",
        template="plotly_dark",
        color_discrete_sequence=["#14b8a6"],
        labels={"total_volume": "Volume ($)", "date": ""},
    )
    volume_chart.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=10),
        yaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
    )
    st.plotly_chart(volume_chart, use_container_width=True)

with category_col:
    st.subheader("Volume by Category")
    category_volume = df.groupby("merchant_category")["total_volume"].sum().reset_index()
    category_chart = px.pie(
        category_volume,
        values="total_volume",
        names="merchant_category",
        hole=0.62,
        template="plotly_dark",
        color_discrete_sequence=["#14b8a6", "#38bdf8", "#a78bfa", "#f59e0b", "#f43f5e", "#22c55e", "#eab308"],
    )
    category_chart.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        margin=dict(t=10, b=10),
    )
    st.plotly_chart(category_chart, use_container_width=True)

count_col, fraud_col = st.columns(2)
with count_col:
    st.subheader("Daily Transaction Count")
    daily_count = df.groupby("date")["transaction_count"].sum().reset_index()
    count_chart = px.bar(
        daily_count,
        x="date",
        y="transaction_count",
        template="plotly_dark",
        color="transaction_count",
        color_continuous_scale="Teal",
        labels={"transaction_count": "Transactions", "date": ""},
    )
    count_chart.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        margin=dict(t=10, b=10),
        yaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
    )
    st.plotly_chart(count_chart, use_container_width=True)

with fraud_col:
    st.subheader("Fraud Suspects by Category")
    fraud_category = (
        df.groupby("merchant_category")["fraud_count"].sum().reset_index().sort_values("fraud_count", ascending=True)
    )
    fraud_chart = px.bar(
        fraud_category,
        x="fraud_count",
        y="merchant_category",
        orientation="h",
        template="plotly_dark",
        color="fraud_count",
        color_continuous_scale="Reds",
        labels={"fraud_count": "Fraud Suspects", "merchant_category": ""},
    )
    fraud_chart.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        margin=dict(t=10, b=10),
        xaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
    )
    st.plotly_chart(fraud_chart, use_container_width=True)

top_col, rolling_col = st.columns([1, 2])
with top_col:
    st.subheader("Top Categories")
    top_categories = df.groupby("merchant_category")["total_volume"].sum().reset_index()
    top_categories = top_categories.sort_values("total_volume", ascending=False)
    top_categories["volume"] = top_categories["total_volume"].apply(lambda value: f"${value:,.0f}")
    st.dataframe(
        top_categories[["merchant_category", "volume"]].rename(
            columns={"merchant_category": "Category", "volume": "Total Volume"}
        ),
        use_container_width=True,
        hide_index=True,
    )

with rolling_col:
    st.subheader("7-Day Rolling Volume")
    rolling = df.groupby("date")["total_volume"].sum().reset_index()
    rolling["rolling_7d"] = rolling["total_volume"].rolling(7, min_periods=1).mean()
    rolling_chart = go.Figure()
    rolling_chart.add_trace(
        go.Bar(x=rolling["date"], y=rolling["total_volume"], name="Daily Volume", marker_color="rgba(20,184,166,0.32)")
    )
    rolling_chart.add_trace(
        go.Scatter(x=rolling["date"], y=rolling["rolling_7d"], name="7-Day Avg", line=dict(color="#f59e0b", width=2.5))
    )
    rolling_chart.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis=dict(gridcolor="rgba(255,255,255,0.06)"),
    )
    st.plotly_chart(rolling_chart, use_container_width=True)

with st.expander("View Gold Table Rows"):
    st.dataframe(df, use_container_width=True, hide_index=True)
