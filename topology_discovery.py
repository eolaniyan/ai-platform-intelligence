import pandas as pd
import networkx as nx
from pyvis.network import Network

# Labels used in graph rendering and dashboard text.
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


def load_logs(log_path: str = "data/service_logs.csv") -> pd.DataFrame:
    """
    Load service logs from disk.
    """
    return pd.read_csv(log_path)


def discover_topology(log_df: pd.DataFrame) -> pd.DataFrame:
    """
    Auto-discover service edges from logs.

    Output columns
    --------------
    source_service
    target_service
    call_count

    Interpretation
    --------------
    source_service -> target_service means:
    source called target in the observed logs.
    """
    edges = (
        log_df.groupby(["source_service", "target_service"])
        .size()
        .reset_index(name="call_count")
        .sort_values("call_count", ascending=False)
    )
    return edges


def build_data_flow_graph(edge_df: pd.DataFrame) -> nx.DiGraph:
    """
    Build a directed graph representing request / data flow.

    Edge direction:
    source_service -> target_service
    """
    graph = nx.DiGraph()

    for _, row in edge_df.iterrows():
        graph.add_edge(
            row["source_service"],
            row["target_service"],
            weight=int(row["call_count"])
        )

    return graph


def build_impact_graph(data_flow_graph: nx.DiGraph) -> nx.DiGraph:
    """
    Build the reverse graph used for blast-radius reasoning.

    Why reverse it?
    ---------------
    If A -> B in the data flow graph, A depends on B.

    So if B fails, A may be impacted.

    Reversing the graph makes impact propagation easier to reason about:
    B -> A in the impact graph means:
    'failure in B may affect A'
    """
    return data_flow_graph.reverse(copy=True)


def service_health_metrics(log_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate simple health metrics by target service.

    These metrics help color the architecture map and explain which
    services look unhealthy.
    """
    metrics = (
        log_df.groupby("target_service")
        .agg(
            avg_latency_ms=("latency_ms", "mean"),
            max_latency_ms=("latency_ms", "max"),
            total_calls=("target_service", "count"),
            error_count=("error", "sum")
        )
        .reset_index()
    )

    metrics["error_rate"] = metrics["error_count"] / metrics["total_calls"]
    return metrics


def _layer_positions_from_graph(data_flow_graph: nx.DiGraph) -> dict:
    """
    Create deterministic x/y positions for a left-to-right architecture map.

    We use topological generations because the graph is a DAG in this simulation.
    That gives us a clean layered diagram:
    entry layer -> auth/risk -> settlement -> reporting

    Returns
    -------
    dict
        Mapping: service_key -> (x, y)
    """
    positions = {}

    # If graph is a DAG, we can place nodes by generation/layer.
    try:
        generations = list(nx.topological_generations(data_flow_graph))
    except Exception:
        # Fallback: single generation if topo sort fails for any reason.
        generations = [list(data_flow_graph.nodes())]

    x_gap = 260
    y_gap = 150

    for layer_index, generation in enumerate(generations):
        x_pos = 120 + layer_index * x_gap
        y_start = 140

        for node_index, node in enumerate(sorted(generation)):
            y_pos = y_start + node_index * y_gap
            positions[node] = (x_pos, y_pos)

    return positions


def render_architecture_map(
    data_flow_graph: nx.DiGraph,
    metrics_df: pd.DataFrame,
    output_html: str = "outputs/architecture_map.html"
) -> None:
    """
    Render a layered architecture map using PyVis.

    The output is interactive and suitable for embedding in Streamlit.
    """

    metric_map = metrics_df.set_index("target_service").to_dict("index")
    positions = _layer_positions_from_graph(data_flow_graph)

    net = Network(
        height="780px",
        width="100%",
        directed=True,
        bgcolor="#ffffff",
        font_color="#222222"
    )

    for node in data_flow_graph.nodes():
        label = SERVICE_LABELS.get(node, node)
        stats = metric_map.get(node, {})

        avg_latency = stats.get("avg_latency_ms", 0)
        error_rate = stats.get("error_rate", 0)
        total_calls = stats.get("total_calls", 0)

        # Node color logic
        if error_rate >= 0.08 or avg_latency >= 250:
            color = "#ff6b6b"   # unhealthy
        elif error_rate >= 0.03 or avg_latency >= 170:
            color = "#ffd166"   # degraded
        else:
            color = "#4dabf7"   # relatively healthy

        title = (
            f"<b>{label}</b><br>"
            f"Average latency: {avg_latency:.1f} ms<br>"
            f"Error rate: {error_rate:.2%}<br>"
            f"Observed calls: {int(total_calls)}"
        )

        x_pos, y_pos = positions.get(node, (0, 0))

        net.add_node(
            node,
            label=label,
            title=title,
            color=color,
            shape="box",
            x=x_pos,
            y=y_pos,
            fixed=True
        )

    for source, target, data in data_flow_graph.edges(data=True):
        net.add_edge(
            source,
            target,
            value=max(int(data.get("weight", 1)), 1),
            title=f"Calls observed: {int(data.get('weight', 1)):,}",
            arrows="to"
        )

    net.set_options("""
    var options = {
      "nodes": {
        "font": { "size": 17, "face": "arial" },
        "shapeProperties": { "borderRadius": 10 },
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

    net.write_html(output_html, open_browser=False)


def build_topology_artifacts(log_path: str = "data/service_logs.csv") -> tuple[nx.DiGraph, nx.DiGraph, pd.DataFrame]:
    """
    Convenience function that:
    - loads logs
    - discovers topology
    - builds both graphs
    - computes health metrics
    - renders the architecture map

    Returns
    -------
    tuple
        (data_flow_graph, impact_graph, metrics_df)
    """
    log_df = load_logs(log_path)
    edge_df = discover_topology(log_df)
    data_flow_graph = build_data_flow_graph(edge_df)
    impact_graph = build_impact_graph(data_flow_graph)
    metrics_df = service_health_metrics(log_df)
    render_architecture_map(data_flow_graph, metrics_df)
    return data_flow_graph, impact_graph, metrics_df


if __name__ == "__main__":
    build_topology_artifacts()
    print("Built architecture map at outputs/architecture_map.html")