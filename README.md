# ⚡ Real-Time Stock Market Streaming Pipeline — Azure

> Kafka + Databricks + Delta Lake (Medallion) + dbt + Power BI
> Built by Ashok Chowdary | Senior Data Engineer

## Architecture


## Impact Metrics

| Metric | Result |
|--------|--------|
| Events/sec processed | 10,000+ |
| End-to-end latency | < 5 seconds |
| Data quality | 99.8% accuracy |
| dbt models | 12 (staging + marts) |
| dbt tests | 100% coverage |

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Apache Kafka / Azure Event Hubs |
| Processing | Databricks Structured Streaming, PySpark |
| Storage | Delta Lake, Azure Data Lake Gen2 |
| Transformation | dbt Core |
| Orchestration | Azure Data Factory |
| BI | Power BI Real-Time Dataset |
| CI/CD | GitHub Actions |
| IaC | Terraform |

## Quick Start

```bash
pip install -r kafka_producer/requirements.txt
docker-compose up -d
python kafka_producer/stock_producer.py
cd dbt_project && dbt run && dbt test
```
