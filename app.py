from log_generator import generate_logs
from dependency_mapper import build_graph
from anomaly_detector import detect_anomalies

if __name__ == "__main__":
    df = generate_logs()
    df.to_csv("data/service_logs.csv", index=False)
    build_graph()
    detect_anomalies()