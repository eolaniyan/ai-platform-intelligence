import pandas as pd

LATENCY_THRESHOLD = 250

def detect_anomalies():
    df = pd.read_csv("data/service_logs.csv")
    anomalies = df[(df["latency_ms"] > LATENCY_THRESHOLD) | (df["error"] == True)].copy()

    summary = anomalies.groupby("target_service").agg(
        anomaly_count=("target_service", "count"),
        avg_latency=("latency_ms", "mean"),
        error_count=("error", "sum")
    ).reset_index()

    summary = summary.sort_values(by="anomaly_count", ascending=False)
    summary.to_csv("outputs/anomaly_summary.csv", index=False)

    print(summary.to_string(index=False))
    print("\nSaved outputs/anomaly_summary.csv")

if __name__ == "__main__":
    detect_anomalies()