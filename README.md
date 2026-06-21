\# Crypto Market Data Pipeline



An end-to-end, automated ELT data pipeline that extracts daily cryptocurrency prices, loads them into a cloud data warehouse, and transforms them into analyst-ready tables using modern data engineering tools.



\## Architecture

CoinGecko API



↓



Python (extract.py)



↓



Google Cloud Storage (raw JSON)



↓



BigQuery — crypto\_raw.raw\_prices (Bronze layer)



↓



dbt staging model — stg\_crypto\_prices (Silver layer)



↓



dbt mart models — fact\_crypto\_prices, dim\_coins (Gold layer)



↓



dbt data quality tests



↓



GitHub Actions — scheduled daily at 8am UTC





\## Tech Stack



\- \*\*Language:\*\* Python

\- \*\*Cloud:\*\* Google Cloud Platform (BigQuery, Cloud Storage)

\- \*\*Transformation:\*\* dbt Core

\- \*\*Orchestration:\*\* GitHub Actions (CI/CD)

\- \*\*Data Modeling:\*\* Star schema (fact + dimension tables), medallion architecture (Bronze/Silver/Gold)



\## Key Engineering Decisions



\- \*\*Idempotent loads\*\* — Pipeline deletes and reloads each day's data to prevent duplicates on reruns

\- \*\*Deduplication logic\*\* — Uses `ROW\_NUMBER()` window functions to handle duplicate extractions

\- \*\*Data quality testing\*\* — dbt tests validate not-null and uniqueness constraints on every run

\- \*\*Medallion architecture\*\* — Clear separation between raw, cleaned, and business-ready data layers

\- \*\*CI/CD automation\*\* — Fully automated daily runs via GitHub Actions, no manual intervention required



\## Pipeline Components



| File | Purpose |

|---|---|

| `extract.py` | Pulls live crypto prices from CoinGecko API, saves locally, uploads to GCS |

| `load\_to\_bigquery.py` | Loads raw data from local JSON into BigQuery with idempotent delete-and-reload logic |

| `crypto\_transforms/` | dbt project containing staging and mart models |

| `.github/workflows/daily\_pipeline.yml` | GitHub Actions workflow that runs the full pipeline daily |



\## Data Model



\*\*fact\_crypto\_prices\*\* (grain: one row per coin per day)

\- price\_id, coin\_id, price\_date, price\_usd, market\_cap, volume\_24h, price\_change\_pct



\*\*dim\_coins\*\* (grain: one row per coin)

\- coin\_id, symbol, name, ath, atl



\## Running Locally



```bash

pip install requests pandas google-cloud-bigquery google-cloud-storage dbt-bigquery

python extract.py

python load\_to\_bigquery.py

cd crypto\_transforms

dbt run

dbt test

```

