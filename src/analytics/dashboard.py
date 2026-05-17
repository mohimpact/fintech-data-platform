import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import random

# ─────────────────────────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fintech Data Platform | Gold Analytics",
    page_icon="💸",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────
# Custom CSS — Premium dark glassmorphism theme
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    .stApp {
        background: linear-gradient(135deg, #0a0e1a 0%, #0d1117 50%, #0a0e1a 100%);
        color: #e2e8f0;
    }
    /* KPI cards */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, rgba(255,255,255,0.05) 0%, rgba(255,255,255,0.02) 100%);
        border: 1px solid rgba(0, 240, 255, 0.15);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4), inset 0 1px 0 rgba(255,255,255,0.1);
        backdrop-filter: blur(10px);
    }
    div[data-testid="metric-container"] label {
        color: #94a3b8 !important;
        font-size: 0.8rem !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }
    div[data-testid="metric-container"] div[data-testid="metric-value"] {
        color: #00f0ff !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }
    /* Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(13, 17, 23, 0.95);
        border-right: 1px solid rgba(0, 240, 255, 0.1);
    }
    /* Section headers */
    h2, h3 { color: #e2e8f0; }
    /* Status badge */
    .status-live {
        display: inline-block;
        background: rgba(0, 255, 136, 0.15);
        border: 1px solid rgba(0, 255, 136, 0.4);
        color: #00ff88;
        border-radius: 20px;
        padding: 2px 12px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
    .status-demo {
        display: inline-block;
        background: rgba(255, 200, 0, 0.15);
        border: 1px solid rgba(255, 200, 0, 0.4);
        color: #ffc800;
        border-radius: 20px;
        padding: 2px 12px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.05em;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────────────────────
def generate_demo_data():
    """Generate realistic mock Gold-layer data for demo purposes."""
    random.seed(42)
    categories = ["Retail", "Food", "Travel", "Electronics", "Entertainment", "Healthcare", "Utilities"]
    records = []
    base_date = datetime.today() - timedelta(days=29)
    for i in range(30):
        date = (base_date + timedelta(days=i)).strftime("%Y-%m-%d")
        for cat in categories:
            vol = random.uniform(10_000, 500_000)
            count = random.randint(50, 2000)
            records.append({
                "date": date,
                "merchant_category": cat,
                "total_volume": round(vol, 2),
                "transaction_count": count,
                "average_transaction_size": round(vol / count, 2),
                "fraud_count": random.randint(0, int(count * 0.05)),
            })
    return pd.DataFrame(records)


@st.cache_data(ttl=60)
def load_gold_data():
    """
    Attempt to load real Gold data from MinIO.
    Falls back to demo data if MinIO is unavailable or no data exists yet.
    """
    try:
        import s3fs
        import pyarrow.parquet as pq

        endpoint = os.getenv("S3_ENDPOINT", "http://127.0.0.1:9000")
        fs = s3fs.S3FileSystem(
            key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
            client_kwargs={"endpoint_url": endpoint}
        )

        gold_path = "warehouse/local/gold/merchant_metrics/data"
        if not fs.exists(gold_path):
            return generate_demo_data(), False   # (data, is_live)

        dataset = pq.ParquetDataset(f"s3://{gold_path}", filesystem=fs)
        df = dataset.read().to_pandas()
        if df.empty:
            return generate_demo_data(), False
        return df, True

    except Exception:
        return generate_demo_data(), False


# ─────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 💸 Fintech Platform")
    st.markdown("---")
    st.markdown("### 📊 Data Source")
    data_source = st.radio("Data Source", ["Auto (MinIO → Demo)", "Demo Mode"], index=0, label_visibility="collapsed")
    st.markdown("---")
    st.markdown("### 🔍 Filters")
    st.markdown("---")
    st.markdown("""
    **Architecture**
    - Kafka → Bronze
    - Spark → Silver
    - Airflow → Gold
    - Streamlit → Analytics
    """)
    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────
col_title, col_badge = st.columns([4, 1])
with col_title:
    st.title("💸 Fintech Data Platform Analytics")
    st.markdown("Real-time insights derived from the Lakehouse **Gold** layer.")
with col_badge:
    st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────
# Load Data
# ─────────────────────────────────────────────────────────────
with st.spinner("Connecting to MinIO Lakehouse..."):
    if data_source == "Demo Mode":
        df, is_live = generate_demo_data(), False
    else:
        df, is_live = load_gold_data()

# Data source badge
if is_live:
    st.markdown('<span class="status-live">● LIVE DATA</span>', unsafe_allow_html=True)
else:
    st.markdown('<span class="status-demo">⚡ DEMO MODE — Run the Airflow ETL to load live data</span>', unsafe_allow_html=True)

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# KPI Row
# ─────────────────────────────────────────────────────────────
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

total_volume    = df["total_volume"].sum()
total_txns      = df["transaction_count"].sum()
avg_txn_size    = df["average_transaction_size"].mean()
total_fraud     = df["fraud_count"].sum() if "fraud_count" in df.columns else 0
fraud_rate      = (total_fraud / total_txns * 100) if total_txns > 0 else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 Total Volume",        f"${total_volume:,.0f}")
k2.metric("🔁 Total Transactions",  f"{total_txns:,}")
k3.metric("📊 Avg Transaction",     f"${avg_txn_size:,.2f}")
k4.metric("🚨 Fraud Rate",          f"{fraud_rate:.2f}%")

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Row 1: Time Series & Donut
# ─────────────────────────────────────────────────────────────
colA, colB = st.columns([2, 1])

with colA:
    st.subheader("📈 Daily Transaction Volume")
    daily = df.groupby("date")["total_volume"].sum().reset_index()
    fig_area = px.area(
        daily, x="date", y="total_volume",
        template="plotly_dark",
        color_discrete_sequence=["#00f0ff"],
        labels={"total_volume": "Volume ($)", "date": ""}
    )
    fig_area.update_traces(
        fill="tozeroy",
        fillcolor="rgba(0,240,255,0.08)",
        line=dict(width=2)
    )
    fig_area.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=10),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)"),
        xaxis=dict(gridcolor="rgba(255,255,255,0.05)")
    )
    st.plotly_chart(fig_area, use_container_width=True)

with colB:
    st.subheader("🥧 Volume by Category")
    cat_vol = df.groupby("merchant_category")["total_volume"].sum().reset_index()
    fig_donut = px.pie(
        cat_vol, values="total_volume", names="merchant_category",
        hole=0.65,
        template="plotly_dark",
        color_discrete_sequence=["#00f0ff","#00ff88","#7c3aed","#f59e0b","#ef4444","#3b82f6"]
    )
    fig_donut.update_traces(textposition="outside", textinfo="percent+label")
    fig_donut.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        margin=dict(t=10, b=10)
    )
    st.plotly_chart(fig_donut, use_container_width=True)

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Row 2: Transaction count bar + Fraud heatmap
# ─────────────────────────────────────────────────────────────
colC, colD = st.columns(2)

with colC:
    st.subheader("🔢 Daily Transaction Count")
    daily_cnt = df.groupby("date")["transaction_count"].sum().reset_index()
    fig_bar = px.bar(
        daily_cnt, x="date", y="transaction_count",
        template="plotly_dark",
        color="transaction_count",
        color_continuous_scale="Teal",
        labels={"transaction_count": "# Transactions", "date": ""}
    )
    fig_bar.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        margin=dict(t=10, b=10),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)")
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with colD:
    st.subheader("🚨 Fraud Count by Category")
    if "fraud_count" in df.columns:
        fraud_cat = df.groupby("merchant_category")["fraud_count"].sum().reset_index().sort_values("fraud_count", ascending=True)
        fig_fraud = px.bar(
            fraud_cat, x="fraud_count", y="merchant_category",
            orientation="h",
            template="plotly_dark",
            color="fraud_count",
            color_continuous_scale="Reds",
            labels={"fraud_count": "Fraud Cases", "merchant_category": ""}
        )
        fig_fraud.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False,
            margin=dict(t=10, b=10),
            xaxis=dict(gridcolor="rgba(255,255,255,0.05)")
        )
        st.plotly_chart(fig_fraud, use_container_width=True)
    else:
        st.info("Fraud data not available in this dataset.")

st.markdown("---")

# ─────────────────────────────────────────────────────────────
# Row 3: Top Merchants & Raw Data
# ─────────────────────────────────────────────────────────────
colE, colF = st.columns([1, 2])

with colE:
    st.subheader("🏆 Top Categories by Volume")
    top = df.groupby("merchant_category")["total_volume"].sum().reset_index()
    top = top.sort_values("total_volume", ascending=False)
    top["volume_fmt"] = top["total_volume"].apply(lambda x: f"${x:,.0f}")
    st.dataframe(
        top[["merchant_category", "volume_fmt"]].rename(columns={
            "merchant_category": "Category",
            "volume_fmt": "Total Volume"
        }),
        use_container_width=True,
        hide_index=True
    )

with colF:
    st.subheader("📅 7-Day Rolling Volume")
    daily_roll = df.groupby("date")["total_volume"].sum().reset_index()
    daily_roll["rolling_7d"] = daily_roll["total_volume"].rolling(7, min_periods=1).mean()
    fig_roll = go.Figure()
    fig_roll.add_trace(go.Bar(
        x=daily_roll["date"], y=daily_roll["total_volume"],
        name="Daily Volume", marker_color="rgba(0,240,255,0.3)"
    ))
    fig_roll.add_trace(go.Scatter(
        x=daily_roll["date"], y=daily_roll["rolling_7d"],
        name="7-Day MA", line=dict(color="#00ff88", width=2.5)
    ))
    fig_roll.update_layout(
        template="plotly_dark",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis=dict(gridcolor="rgba(255,255,255,0.05)")
    )
    st.plotly_chart(fig_roll, use_container_width=True)

st.markdown("---")

# Raw data expander
with st.expander("🗃️ View Raw Gold Table Data"):
    st.dataframe(df, use_container_width=True, hide_index=True)

# Footer
st.markdown("""
<div style='text-align:center; color:#475569; font-size:0.8rem; padding: 20px 0;'>
    Fintech Data Platform · Medallion Architecture · Bronze → Silver → Gold · Powered by Streamlit
</div>
""", unsafe_allow_html=True)
