import pandas as pd


def detect_anomalies(log_path: str = "data/service_logs.csv") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Detect anomalies from service logs.

    Strategy
    --------
    A row is considered anomalous if:
    - latency is far above the overall mean
    OR
    - an explicit error occurred

    Outputs
    -------
    anomaly_events.csv
        row-level suspicious events

    anomaly_summary.csv
        service-level summary used by the dashboard and root-cause module
    """

    log_df = pd.read_csv(log_path)

    mean_latency = log_df["latency_ms"].mean()
    std_latency = log_df["latency_ms"].std()

    log_df["latency_anomaly"] = log_df["latency_ms"] > (mean_latency + 2 * std_latency)
    log_df["error_anomaly"] = log_df["error"] == True
    log_df["anomaly"] = log_df["latency_anomaly"] | log_df["error_anomaly"]

    anomaly_events_df = log_df[log_df["anomaly"]].copy()
    anomaly_events_df.to_csv("outputs/anomaly_events.csv", index=False)

    anomaly_summary_df = (
        anomaly_events_df.groupby(["target_service", "target_label"])
        .agg(
            anomaly_count=("target_service", "count"),
            avg_latency_ms=("latency_ms", "mean"),
            max_latency_ms=("latency_ms", "max"),
            error_count=("error", "sum"),
            status_5xx=("status_code", lambda s: int(((s >= 500) & (s <= 599)).sum()))
        )
        .reset_index()
    )

    service_totals_df = (
        log_df.groupby("target_service")
        .size()
        .reset_index(name="total_calls")
    )

    anomaly_summary_df = anomaly_summary_df.merge(
        service_totals_df,
        on="target_service",
        how="left"
    )

    anomaly_summary_df["anomaly_rate"] = anomaly_summary_df["anomaly_count"] / anomaly_summary_df["total_calls"]
    anomaly_summary_df["error_rate_within_anomalies"] = anomaly_summary_df["error_count"] / anomaly_summary_df["anomaly_count"]

    # Simple composite severity score for ranking "most suspicious" services.
    max_latency_value = anomaly_summary_df["avg_latency_ms"].max()
    max_5xx_value = anomaly_summary_df["status_5xx"].max()

    latency_component = anomaly_summary_df["avg_latency_ms"] / max_latency_value if max_latency_value > 0 else 0
    error_component = anomaly_summary_df["status_5xx"] / max_5xx_value if max_5xx_value > 0 else 0

    anomaly_summary_df["severity_score"] = (
        anomaly_summary_df["anomaly_rate"] * 45
        + latency_component * 30
        + error_component * 25
    ).round(2)

    anomaly_summary_df = anomaly_summary_df.sort_values(
        by=["severity_score", "anomaly_count", "avg_latency_ms"],
        ascending=False
    )

    anomaly_summary_df.to_csv("outputs/anomaly_summary.csv", index=False)

    return anomaly_events_df, anomaly_summary_df


if __name__ == "__main__":
    detect_anomalies()
    print("Wrote outputs/anomaly_events.csv and outputs/anomaly_summary.csv")