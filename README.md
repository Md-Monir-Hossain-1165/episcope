# EpiScope — Big Data Surveillance Platform

<!-- VIDEO LINK: [Add YouTube/Drive link here after recording] -->

## Quickstart (One Command)
```bash
# Clone and run the full pipeline
git clone <repo-url>
cd episcope
cp .env.example .env
pip install -r requirements.txt
make all
```

## Prerequisites
- Python 3.10+
- Java 11+ (for PySpark)
- 16GB RAM recommended

## Architecture
```
Raw Sources (S1-S6)
        ↓
   Bronze Layer (raw, immutable, with metadata)
        ↓
   Silver Layer (cleaned, de-identified, validated)
        ↓
    Gold Layer (aggregated weekly per district/disease)
        ↓
 Analytics (Detection | Forecasting | Optimiser | Calendar)
        ↓
  Serving Layer (suppressed JSON exports, ≤10MB)
        ↓
   D3.js Dashboard (choropleth, time series, heatmap, allocation, alerts)
```

## How to Run the Hidden Test Drop
```bash
# Drop new incremental files into pipeline/bronze/encounters/incremental/
# Then re-run the pipeline — it is fully idempotent
make run-incremental
```

## Refresh Dashboard Data
After running the Gold and analytics stages, regenerate the static dashboard exports:

```bash
python serving/export.py
```

This writes the latest privacy-safe JSON and simplified district boundaries to
`dashboard/data/`. Serve the dashboard locally with `cd dashboard && python -m http.server 8000`.

## Requirement Traceability
| Requirement ID | File / Notebook | Report Section |
|---|---|---|
| ING-1 | pipeline/bronze/ingest.py | §4 |
| ING-2 | pipeline/silver/transform.py | §4 |
| ING-3 | pipeline/gold/mart.py | §4 |
| ING-4 | pipeline/silver/incremental.py | §4 |
| DQ-1 | pipeline/quality/rules.py | §4 |
| AN-1 | analytics/detection/ears.py | §5 |
| AN-4 | analytics/forecast/model.py | §5 |
| AN-7 | analytics/optimise/allocate.py | §5 |
| VIS-1 | dashboard/index.html | §7 |

## Project Structure
```
episcope/
├── README.md
├── AI_USAGE.md
├── requirements.txt
├── Makefile
├── .env.example
├── generator/          # Synthetic data generator (S4-S6)
├── pipeline/
│   ├── bronze/         # Raw ingested data
│   ├── silver/         # Cleaned, de-identified data
│   ├── gold/           # Aggregated analytics-ready data
│   └── quality/        # Quality rules and reports
├── analytics/
│   ├── detection/      # Outbreak detection (EARS/CUSUM)
│   ├── forecast/       # 1-4 week forecasting
│   ├── optimise/       # Resource allocation optimiser
│   └── calendar/       # Seasonal preparedness calendar
├── serving/            # Gold → suppressed JSON exports
├── dashboard/          # D3.js v7 dashboard
├── tests/              # Automated tests
└── docs/               # Report, proposal, policy brief
```
