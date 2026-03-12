import os
import pandas as pd
import networkx as nx
from dotenv import load_dotenv
from topology_discovery import build_topology_artifacts, SERVICE_LABELS

load_dotenv()


# Plain-English business impact descriptions used in fallback summaries.
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


def compute_blast_radius(impact_graph: nx.DiGraph, service_key: str) -> list[str]:
    """
    Compute the blast radius using the impact graph.

    In the impact graph:
    service -> impacted_service

    So descendants of the selected service are the services likely to be affected
    if that service degrades.
    """
    try:
        impacted = sorted(nx.descendants(impact_graph, service_key))
        return impacted
    except Exception:
        return []


def fallback_summary(anomaly_summary_df: pd.DataFrame, impact_graph: nx.DiGraph) -> str:
    """
    Deterministic, non-LLM summary.

    Used when no OPENAI_API_KEY is available or if the API call fails.
    """

    if anomaly_summary_df.empty:
        return "No anomalies were detected, so no root cause analysis could be generated."

    worst_row = anomaly_summary_df.iloc[0]
    service_key = worst_row["target_service"]
    service_label = SERVICE_LABELS.get(service_key, service_key)

    blast_radius = compute_blast_radius(impact_graph, service_key)
    blast_labels = [SERVICE_LABELS.get(s, s) for s in blast_radius]

    lines = [
        "AI Root Cause Summary",
        "",
        f"The service with the strongest anomaly signature is **{service_label}**.",
        "",
        "Why it stands out:",
        f"- It recorded {int(worst_row['anomaly_count'])} anomalous events.",
        f"- Its average anomalous latency was {worst_row['avg_latency_ms']:.1f} ms.",
        f"- It produced {int(worst_row['status_5xx'])} server-side 5xx failures.",
        f"- Its combined severity score was {worst_row['severity_score']:.2f}.",
        "",
        "Likely business impact:",
        f"- {BUSINESS_IMPACT_MAP.get(service_key, 'No business impact description available.')}",
        "",
        "Likely blast radius:"
    ]

    if blast_labels:
        for label in blast_labels:
            lines.append(f"- {label}")
    else:
        lines.append("- No significant downstream impact was inferred from the current topology.")

    lines.extend([
        "",
        "Plain-English conclusion:",
        f"{service_label} looks like the most likely starting point of the incident. "
        "This is where engineers would usually investigate first."
    ])

    return "\n".join(lines)


def summarize_root_cause(
    anomaly_summary_path: str = "outputs/anomaly_summary.csv"
) -> str:
    """
    Generate a root-cause explanation.

    Logic
    -----
    1. Load anomaly summary
    2. Rebuild topology artifacts to get the latest impact graph
    3. If OPENAI_API_KEY exists, ask an LLM to explain the issue
    4. Otherwise use deterministic fallback logic
    5. Save result to outputs/root_cause_summary.txt

    Returns
    -------
    str
        Final root-cause explanation text.
    """

    if not os.path.exists(anomaly_summary_path):
        text = "No anomaly summary found. Run anomaly detection first."
        with open("outputs/root_cause_summary.txt", "w", encoding="utf-8") as f:
            f.write(text)
        return text

    anomaly_summary_df = pd.read_csv(anomaly_summary_path)
    _, impact_graph, _ = build_topology_artifacts()

    api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        text = fallback_summary(anomaly_summary_df, impact_graph)
        with open("outputs/root_cause_summary.txt", "w", encoding="utf-8") as f:
            f.write(text)
        return text

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        summary_text = anomaly_summary_df.head(12).to_string(index=False)

        prompt = f"""
You are an AI observability assistant for a fintech platform.

Based on this anomaly summary, explain:
1. Which service is most likely the root cause
2. Which services are likely to be affected
3. What engineers should investigate first
4. Explain the situation in plain English for both technical and non-technical readers

Anomaly summary:
{summary_text}
"""

        response = client.responses.create(
            model="gpt-4.1-mini",
            input=prompt
        )

        text = response.output_text

    except Exception as exc:
        text = fallback_summary(anomaly_summary_df, impact_graph)
        text += f"\n\n(LLM call failed, so fallback summary was used. Details: {exc})"

    with open("outputs/root_cause_summary.txt", "w", encoding="utf-8") as f:
        f.write(text)

    return text


if __name__ == "__main__":
    result = summarize_root_cause()
    print(result)