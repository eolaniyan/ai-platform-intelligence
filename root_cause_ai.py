import os
import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

def summarize_root_cause():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("No OPENAI_API_KEY found. Skipping AI summary.")
        return

    client = OpenAI(api_key=api_key)
    summary_df = pd.read_csv("outputs/anomaly_summary.csv")
    summary_text = summary_df.to_string(index=False)

    prompt = f"""
You are an AI observability assistant.

Based on this anomaly summary from a distributed payments platform, identify the most likely root cause and explain probable downstream effects.

Anomaly summary:
{summary_text}
"""

    response = client.responses.create(
        model="gpt-4.1-mini",
        input=prompt
    )

    output = response.output_text
    with open("outputs/root_cause_summary.txt", "w", encoding="utf-8") as f:
        f.write(output)

    print(output)
    print("\nSaved outputs/root_cause_summary.txt")

if __name__ == "__main__":
    summarize_root_cause()