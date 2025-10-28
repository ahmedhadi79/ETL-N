# AllFunds ETL — README

* **Purpose:** ETL pipeline to fetch AllFunds data and write partitioned Parquet to S3 (Glue/Athena friendly).
* **Entry point:** `main()` (script expects Glue runtime args or use `_MANUAL_EVENT` for local/testing).
* **Date handling:** `start_date`/`end_date` (YYYYMMDD); defaults to previous UTC day when absent.
* **Supported tables:** `allfunds_transactions_open_positions`, `allfunds_transactions_performance`.
* **Local override:** `_MANUAL_EVENT` allows specifying `athena_table_names`, `portfolio_ids`, `start_date`, `end_date` for testing.

## Workflow
```mermaid
flowchart
  A[Load runtime args] --> B[Init API client]
  B --> C[Build date list]
  C --> D[Fetch portfolio ids]
  D --> E{For each table / portfolio / date}
  E -->|open_positions| F[POST /api/v1/analysis/positions/transactions]
  E -->|performance| G[POST /api/v1/analysis/performance/transactions]
  F --> H["Normalize records: add date, portfolio_id, timestamp_extracted"]
  G --> H
  H --> I[Concatenate -> DataFrame]
  I --> J["Save to S3 parquet partitioned by date & update Glue"]
  J --> K[Aggregate processed/failed counts]
  K --> L{Any failures?}
  L -->|no| M[Return status: completed]
  L -->|yes| N[Raise RuntimeError / Return partial]
```

## Architecture
![Alt text](allfunds_transactions_open_positions.png)
