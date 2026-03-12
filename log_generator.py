import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Fixed seed so the simulation is repeatable enough for demos.
np.random.seed(42)

# Human-readable service labels.
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

# Canonical service-to-service request paths.
# source_service -> target_service means:
# "source calls target"
DATA_FLOW_EDGES = [
    ("merchant_portal", "payment_api"),
    ("payment_api", "auth_service"),
    ("auth_service", "risk_engine"),
    ("auth_service", "fraud_engine"),
    ("risk_engine", "settlement_service"),
    ("fraud_engine", "settlement_service"),
    ("settlement_service", "ledger_service"),
    ("settlement_service", "notification_service"),
    ("ledger_service", "reconciliation_service"),
    ("reconciliation_service", "reporting_service"),
]

# Approximate healthy latency baselines in milliseconds.
BASE_LATENCY_MS = {
    "merchant_portal": 75,
    "payment_api": 105,
    "auth_service": 95,
    "risk_engine": 120,
    "fraud_engine": 130,
    "settlement_service": 160,
    "ledger_service": 100,
    "reconciliation_service": 145,
    "reporting_service": 90,
    "notification_service": 80,
}


def generate_logs(n_events: int = 80000) -> pd.DataFrame:
    """
    Generate synthetic distributed-system traffic.

    Parameters
    ----------
    n_events : int
        Number of service-to-service events to create.

    Returns
    -------
    pd.DataFrame
        A dataframe of synthetic platform log records.

    Notes
    -----
    This simulation deliberately injects:
    - normal traffic
    - a large incident window
    - smaller random anomalies outside that window

    That helps the dashboard look realistic and produces multiple anomalies,
    rather than a single obvious outlier.
    """

    start_time = datetime.now()
    rows = []

    # Simulate one larger incident affecting the middle of the timeline.
    incident_start = int(n_events * 0.45)
    incident_end = int(n_events * 0.62)

    for i in range(n_events):
        source_service, target_service = DATA_FLOW_EDGES[np.random.randint(0, len(DATA_FLOW_EDGES))]

        # Timestamp spacing is small so the dataset feels dense.
        timestamp = start_time + timedelta(seconds=i * 2)

        healthy_latency = BASE_LATENCY_MS[target_service]
        latency_ms = np.random.normal(healthy_latency, healthy_latency * 0.16)

        error = False
        status_code = 200

        # Main incident logic:
        # settlement and downstream finance services degrade hardest.
        if incident_start <= i <= incident_end:
            if target_service in {"settlement_service", "ledger_service", "reconciliation_service"}:
                latency_ms *= np.random.uniform(2.8, 5.5)
                error = np.random.rand() < 0.28
            elif target_service in {"payment_api", "auth_service", "risk_engine", "fraud_engine"}:
                latency_ms *= np.random.uniform(1.4, 2.2)
                error = np.random.rand() < 0.12

        # Smaller ambient anomalies outside the main incident.
        if np.random.rand() < 0.015:
            latency_ms *= np.random.uniform(1.7, 3.0)
            error = np.random.rand() < 0.15

        if error:
            status_code = np.random.choice([500, 502, 503, 504, 408])
        elif latency_ms > healthy_latency * 2.0:
            # Not fully failed, but clearly degraded.
            status_code = 206

        rows.append({
            "timestamp": timestamp,
            "source_service": source_service,
            "target_service": target_service,
            "source_label": SERVICE_LABELS[source_service],
            "target_label": SERVICE_LABELS[target_service],
            "latency_ms": round(max(latency_ms, 5), 2),
            "error": bool(error),
            "status_code": int(status_code),
        })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    df = generate_logs()
    df.to_csv("data/service_logs.csv", index=False)
    print(f"Generated data/service_logs.csv with {len(df):,} rows")