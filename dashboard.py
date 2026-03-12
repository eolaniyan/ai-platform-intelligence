import os
import pandas as pd
import streamlit as st
import networkx as nx
import streamlit.components.v1 as components

from app import run_pipeline
from topology_discovery import (
    build_topology_artifacts,
    SERVICE_LABELS
)

LOG_PATH = "data/service_logs.csv"
MAP_PATH = "outputs/architecture_map.html"
ANOMALY_EVENTS_PATH = "outputs/anomaly_events.csv"
ANOMALY_SUMMARY_PATH = "outputs/anomaly_summary.csv"
ROOT_CAUSE_PATH = "outputs/root_cause_summary.txt"

BUSINESS_IMPACT_MAP = {
    "merchant_portal": "Users may struggle to start checkout or payment journeys.",
    "payment_api": "Checkout requests and merchant-side transaction calls may fail or slow down.",
    "auth_service": "Payment authorization may become delayed, unreliable, or fail.",
    "risk_engine": "Risk decisions may slow down approvals and block downstream transaction steps.",
    "fraud_engine": "Fraud screening delays may hold up settlement or payment completion.",
    "settlement_service": "Settlement, ledger updates, reconciliation, and merchant payout flows may be delayed.",
    "ledger_service": "Ledger consistency and downstream financial reporting may be impacted.",
    "reconciliation_service": "Back-office reconciliation and finance reporting may lag or fail.",
    "reporting_service": "Operational and merchant reports may become stale or incomplete.",
    "notification_service": "Merchant or customer notifications may be delayed."
}


def ensure_artifacts_exist(n_events: int = 80000):
    """
    If outputs are missing, generate them.
    """
    required_files = [LOG_PATH, MAP_PATH, ANOMALY_SUMMARY_PATH, ROOT_CAUSE_PATH]
    if not all(os.path.exists(path) for path in required_files):
        run_pipeline(n_events=n_events)


def safe_read_csv(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        return pd.read_csv(path)
    return pd.DataFrame()


def compute_service_views(data_flow_graph: nx.DiGraph, impact_graph: nx.DiGraph, service_key: str):
    """
    Return the key graph-based views used in the dependency explorer.

    Definitions
    -----------
    In the data flow graph:
    A -> B means A calls B

    So for a selected service:
    - direct dependencies = successors in the data flow graph
      (things this service calls / relies on)

    - direct callers = predecessors in the data flow graph
      (things that call this service)

    In the impact graph:
    - descendants = likely blast radius if this service degrades
    """
    direct_dependencies = sorted(list(data_flow_graph.successors(service_key)))
    direct_callers = sorted(list(data_flow_graph.predecessors(service_key)))
    blast_radius = sorted(list(nx.descendants(impact_graph, service_key)))

    return direct_dependencies, direct_callers, blast_radius


st.set_page_config(page_title="AI Platform Intelligence", layout="wide")
st.title("AI Platform Intelligence")

st.markdown("""
### What this project does

This prototype simulates how engineers and AI systems can understand a complex platform by combining:

- **auto-discovered architecture from logs**
- **service dependency analysis**
- **anomaly detection**
- **blast-radius reasoning**
- **root-cause explanation**

### Important idea

This tool separates **data flow** from **impact propagation**:

- **Data flow** shows who calls whom
- **Impact propagation** shows who is likely affected if a service fails

That is why the dependency explorer can explain the system in both directions.
""")

with st.sidebar:
    st.header("Controls")
    event_count = st.selectbox("Simulated event volume", [20000, 50000, 80000, 120000], index=2)

    if st.button("Run / Refresh Full Analysis"):
        with st.spinner("Generating logs, discovering architecture, detecting anomalies, and writing root cause summary..."):
            run_pipeline(n_events=event_count)
        st.success("Analysis completed successfully.")

# Make sure outputs exist before trying to display them.
ensure_artifacts_exist()

# Rebuild in-memory graph objects for dependency explorer.
data_flow_graph, impact_graph, metrics_df = build_topology_artifacts()

log_df = safe_read_csv(LOG_PATH)
anomaly_summary_df = safe_read_csv(ANOMALY_SUMMARY_PATH)
anomaly_events_df = safe_read_csv(ANOMALY_EVENTS_PATH)

top_left, top_right = st.columns([1, 1])

with top_left:
    st.subheader("System Overview")

    if not log_df.empty:
        total_events = len(log_df)
        unique_services = log_df["target_service"].nunique()
        error_count = int(log_df["error"].sum())
        avg_latency = float(log_df["latency_ms"].mean())

        c1, c2 = st.columns(2)
        c1.metric("Events Analysed", f"{total_events:,}")
        c2.metric("Unique Services", f"{unique_services}")

        c3, c4 = st.columns(2)
        c3.metric("Error Events", f"{error_count:,}")
        c4.metric("Average Latency", f"{avg_latency:.1f} ms")

        st.markdown("#### Average Latency by Service")
        latency_chart = log_df.groupby("target_label")["latency_ms"].mean().sort_values(ascending=False)
        st.bar_chart(latency_chart)

with top_right:
    st.subheader("How to read this dashboard")
    st.write("""
    - The **architecture map** shows request flow between services.
    - The **dependency explorer** shows both:
      - what a service calls
      - what calls that service
    - The **blast radius** view estimates which services could be affected if the selected service degrades.
    - The **root cause summary** explains where the incident most likely started.
    """)

st.markdown("---")
st.subheader("Architecture Map")

if os.path.exists(MAP_PATH):
    with open(MAP_PATH, "r", encoding="utf-8") as f:
        html = f.read()
    components.html(html, height=800, scrolling=True)
else:
    st.info("Architecture map not available yet.")

st.markdown("---")
st.subheader("Dependency Explorer")

selected_service = st.selectbox(
    "Choose a service",
    options=sorted(list(data_flow_graph.nodes())),
    format_func=lambda x: SERVICE_LABELS.get(x, x)
)

direct_dependencies, direct_callers, blast_radius = compute_service_views(
    data_flow_graph,
    impact_graph,
    selected_service
)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("#### This service directly depends on")
    if direct_dependencies:
        for service in direct_dependencies:
            st.write(f"- {SERVICE_LABELS.get(service, service)}")
    else:
        st.write("No direct downstream dependency was detected in the current topology.")

with col2:
    st.markdown("#### Services that directly call this one")
    if direct_callers:
        for service in direct_callers:
            st.write(f"- {SERVICE_LABELS.get(service, service)}")
    else:
        st.write("No direct callers were detected in the current topology.")

with col3:
    st.markdown("#### Likely blast radius if this service degrades")
    if blast_radius:
        for service in blast_radius:
            st.write(f"- {SERVICE_LABELS.get(service, service)}")
    else:
        st.write("No meaningful blast radius was inferred from the current topology.")

st.markdown("#### Plain-English business impact")
st.write(BUSINESS_IMPACT_MAP.get(selected_service, "No business impact description available."))

if not anomaly_summary_df.empty:
    selected_service_row = anomaly_summary_df[anomaly_summary_df["target_service"] == selected_service]

    st.markdown("#### Health signals for selected service")
    if not selected_service_row.empty:
        row = selected_service_row.iloc[0]

        m1, m2, m3 = st.columns(3)
        m1.metric("Anomaly Count", int(row["anomaly_count"]))
        m2.metric("Average Anomalous Latency", f"{row['avg_latency_ms']:.1f} ms")
        m3.metric("Severity Score", f"{row['severity_score']:.2f}")
    else:
        st.info("No anomaly summary row exists for this service in the current run.")

st.markdown("---")
bottom_left, bottom_right = st.columns([1.2, 0.8])

with bottom_left:
    st.subheader("Detected Anomalies")

    if not anomaly_summary_df.empty:
        st.dataframe(anomaly_summary_df, use_container_width=True)

        st.markdown("#### Highest Severity Services")
        severity_chart = anomaly_summary_df.set_index("target_label")["severity_score"].sort_values(ascending=False).head(10)
        st.bar_chart(severity_chart)

        with st.expander("What the anomaly table means"):
            st.write("""
            - **anomaly_count** = how many suspicious events were seen for that service
            - **avg_latency_ms** = how slow the service was during anomalous periods
            - **max_latency_ms** = worst observed anomalous latency
            - **status_5xx** = server-side failures
            - **anomaly_rate** = proportion of total calls that looked abnormal
            - **severity_score** = combined ranking signal for prioritization
            """)

with bottom_right:
    st.subheader("AI Root Cause Summary")

    if os.path.exists(ROOT_CAUSE_PATH):
        with open(ROOT_CAUSE_PATH, "r", encoding="utf-8") as f:
            st.write(f.read())
    else:
        st.info("Root cause summary not available yet.")

    if not anomaly_events_df.empty:
        st.markdown("#### Sample Anomaly Events")
        sample_cols = ["timestamp", "source_label", "target_label", "latency_ms", "status_code", "error"]
        existing_cols = [col for col in sample_cols if col in anomaly_events_df.columns]
        st.dataframe(anomaly_events_df[existing_cols].head(20), use_container_width=True)