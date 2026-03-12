import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

def build_graph():
    df = pd.read_csv("data/service_logs.csv")
    G = nx.DiGraph()

    for _, row in df.iterrows():
        G.add_edge(row["service"], row["target_service"])

    plt.figure(figsize=(10, 6))
    pos = nx.spring_layout(G, seed=42)
    nx.draw(G, pos, with_labels=True, node_size=2500, font_size=9)
    plt.title("Platform Service Dependency Graph")
    plt.savefig("outputs/dependency_graph.png", bbox_inches="tight")
    print("Saved outputs/dependency_graph.png")

if __name__ == "__main__":
    build_graph()