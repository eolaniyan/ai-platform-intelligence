import pandas as pd
from pyvis.network import Network

# Human-readable labels for services.
SERVICE_LABELS = {
    "merchant_portal": "Merchant Portal",
    "payment_api": "Payment API",
    "auth_service": "Authorization Service",
    "risk_engine": "Risk Engine",
    "fraud_engine": "Fraud Engine",
    "settlement_service": "Settlement Service",
    "ledger_service": "Ledger Service",
    "reconciliation_service": "Reconciliation Service",
    "reporting_service": "Reporting Service",
    "notification_service": "Notification Service"
}

# Layer assignments for architecture-map style presentation.
# This is what makes the graph feel more like a true architecture diagram
# instead of a random network blob.
SERVICE_LAYERS = {
    "merchant_portal": 1,
    "payment_api": 2,
    "auth_service": 3,
    "risk_engine": 4,
    "fraud_engine": 4,
    "settlement_service": 5,
    "ledger_service": 6,
    "reconciliation_service": 7,
    "reporting_service": 8,
    "notification_service": 6
}

# Simple x/y coordinates for a more explanatory left-to-right layout.
# You can tweak these later if you want the diagram to look cleaner still.
NODE_POSITIONS = {
    "merchant_portal": (50, 180),
    "payment_api": (220, 180),
    "auth_service": (420, 180),
    "risk_engine": (650, 120),
    "fraud_engine": (650, 250),
    "settlement_service": (930, 180),
    "ledger_service": (1180, 120),
    "notification_service": (1180, 260),
    "reconciliation_service": (1430, 120),
    "reporting_service": (1680, 120),
}


def build_graph() -> None:
    """
    Build an interactive service dependency graph.

    Output
    ------
    outputs/dependency_graph.html

    Design goals
    ------------
    - show direction of dependencies
    - show approximate architecture flow left-to-right
    - color services based on health / anomaly posture
    - make hover info useful for users and interviewers
    """

    df = pd.read_csv("data/service_logs.csv")

    # Count how many times each dependency edge appeared.
    edge_counts = (
        df.groupby(["service", "target_service"])
        .size()
        .reset_index(name="call_count")
    )

    # Aggregate health metrics by target service.
    error_stats = (
        df.groupby("target_service")
        .agg(
            avg_latency=("latency_ms", "mean"),
            error_count=("error", "sum"),
            total_calls=("error", "count")
        )
        .reset_index()
    )

    stats_map = error_stats.set_index("target_service").to_dict("index")

    # Turn off physics because we want a more controlled architecture-style layout.
    net = Network(
        height="760px",
        width="100%",
        directed=True,
        bgcolor="#ffffff",
        font_color="#222222"
    )

    for service_key, label in SERVICE_LABELS.items():
        stats = stats_map.get(service_key, {})
        avg_latency = stats.get("avg_latency", 0)
        error_count = stats.get("error_count", 0)
        total_calls = stats.get("total_calls", 0)
        error_rate = (error_count / total_calls) if total_calls else 0

        # Tooltip content when hovering the node.
        title = (
            f"<b>{label}</b><br>"
            f"Average latency: {avg_latency:.1f} ms<br>"
            f"Error count: {int(error_count)}<br>"
            f"Total calls: {int(total_calls)}<br>"
            f"Error rate: {error_rate:.2%}<br>"
            f"Architecture layer: {SERVICE_LAYERS.get(service_key, 'N/A')}"
        )

        # Color logic:
        # - red = unhealthy / suspicious
        # - yellow = somewhat degraded
        # - blue = relatively healthy
        if error_rate >= 0.08 or avg_latency >= 250:
            color = "#ff6b6b"
        elif error_rate >= 0.03 or avg_latency >= 170:
            color = "#ffd166"
        else:
            color = "#4dabf7"

        x_pos, y_pos = NODE_POSITIONS[service_key]

        net.add_node(
            service_key,
            label=label,
            title=title,
            color=color,
            shape="box",
            size=26,
            x=x_pos,
            y=y_pos,
            fixed=True
        )

    for _, row in edge_counts.iterrows():
        src = row["service"]
        dst = row["target_service"]
        call_count = int(row["call_count"])

        net.add_edge(
            src,
            dst,
            value=max(call_count, 1),
            title=f"Calls observed: {call_count:,}",
            arrows="to"
        )

    net.set_options("""
    var options = {
      "nodes": {
        "font": { "size": 17, "face": "arial" },
        "shapeProperties": { "borderRadius": 8 },
        "borderWidth": 2
      },
      "edges": {
        "smooth": { "enabled": true, "type": "cubicBezier" },
        "arrows": { "to": { "enabled": true, "scaleFactor": 0.8 } }
      },
      "interaction": {
        "hover": true,
        "navigationButtons": true,
        "keyboard": true
      },
      "physics": { "enabled": false }
    }
    """)

    net.write_html("outputs/dependency_graph.html", open_browser=False)
    print("Saved outputs/dependency_graph.html")


if __name__ == "__main__":
    build_graph()