# AI Platform Intelligence

A prototype AI-assisted observability tool for understanding service dependencies and anomaly patterns in a distributed platform.

## What it does
- Generates synthetic service-to-service logs
- Builds a dependency graph between platform modules
- Detects latency spikes and errors
- Optionally uses an LLM to summarize likely root cause

## Example use cases
- Architecture discovery
- Dependency mapping
- Root cause assistance
- Platform observability

## Stack
- Python
- pandas
- networkx
- matplotlib
- OpenAI API (optional)

## Run
```bash
python app.py