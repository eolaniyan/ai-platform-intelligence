from log_generator import generate_logs
from topology_discovery import build_topology_artifacts
from anomaly_detector import detect_anomalies
from root_cause_ai import summarize_root_cause


def run_pipeline(n_events: int = 80000) -> None:
    """
    Run the full Platform Intelligence pipeline end-to-end.

    Steps
    -----
    1. Generate synthetic service logs
    2. Discover topology and render architecture map
    3. Detect anomalies
    4. Generate root cause summary
    """

    log_df = generate_logs(n_events=n_events)
    log_df.to_csv("data/service_logs.csv", index=False)

    build_topology_artifacts()
    detect_anomalies()
    summarize_root_cause()


if __name__ == "__main__":
    run_pipeline()
    print("Platform Intelligence pipeline complete.")