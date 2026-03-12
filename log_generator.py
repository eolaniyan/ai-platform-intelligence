import pandas as pd
import numpy as np
from datetime import datetime, timedelta

np.random.seed(42)

SERVICES = [
    "merchant_portal",
    "payment_api",
    "auth_service",
    "risk_engine",
    "settlement_service",
    "reporting_service"
]

EDGES = [
    ("merchant_portal", "payment_api"),
    ("payment_api", "auth_service"),
    ("auth_service", "risk_engine"),
    ("risk_engine", "settlement_service"),
    ("settlement_service", "reporting_service")
]

def generate_logs(n=500):
    start = datetime.now()
    rows = []

    for i in range(n):
        service, target = EDGES[np.random.randint(0, len(EDGES))]
        latency = np.random.normal(120, 30)
        error = False

        if target == "settlement_service" and np.random.rand() < 0.18:
            latency = np.random.normal(400, 70)
            error = np.random.rand() < 0.5

        rows.append({
            "timestamp": start + timedelta(seconds=i * 10),
            "service": service,
            "target_service": target,
            "latency_ms": max(10, round(latency, 2)),
            "error": error
        })

    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = generate_logs()
    df.to_csv("data/service_logs.csv", index=False)
    print("Generated data/service_logs.csv")