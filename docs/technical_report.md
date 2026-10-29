# EpiScope: A Scalable Disease Surveillance and Response Platform for Bangladesh

**Author 1: [Full name]** — [Student ID] — [Email]  
**Author 2: [Full name]** — [Student ID] — [Email]  
**Author 3: [Full name]** — [Student ID] — [Email]  
**Course:** CSE4345 — Big Data Analytics  
**Date:** October 2026

> Replace bracketed author information before submission. Apply the final IEEE
> Word or LaTeX template after the group approves the technical content.

## Abstract

Routine surveillance, weather, facility, and logistics data are commonly held
in separate systems. EpiScope is a reproducible Bangladesh district-level
prototype that combines these inputs to support early warning and preparedness
planning. A PySpark Bronze–Silver–Gold pipeline ingests, cleans, validates, and
de-identifies approximately 6.0 million synthetic encounter records before
producing weekly analytical marts. EARS C2 detection, SARIMA forecasting,
PuLP-based allocation, and a seasonal preparedness calendar are then served in
a static D3.js dashboard. The completed run produced 5,887,109 clean Silver
records, 83,127 epidemiological-week mart records, 2,000 alert records, 1,280
forecast records, and 64 allocation recommendations. The evaluated scale-factor
0.25 run processed 6,037,903 records in 10.69 seconds. EpiScope demonstrates a
transparent, aggregate-only workflow for district surveillance review; it does
not automate outbreak declaration or replace epidemiological judgement.

**Index Terms—** disease surveillance, Apache Spark, EARS C2, SARIMA, linear
programming, Bangladesh, public-health dashboard.

## I. Introduction

Bangladesh faces seasonal and geographically uneven infectious-disease burden.
Decision-makers need a connected view of reported cases, abnormal increases,
near-term demand, and supply priorities. EpiScope addresses this gap through a
district-level analytics platform for dengue, diarrhoeal disease,
influenza-like illness, malaria, and a fifth disease grouping in the generated
data. The platform is decision support: alerts prompt review, forecasts support
planning, and allocation outputs are recommendations rather than instructions.

This project contributes (1) a reproducible Spark pipeline, (2) integrated
early-warning, forecasting, optimisation, and seasonal methods, (3) a
responsive D3.js dashboard that exposes only aggregate outputs, and (4) scale
measurements that assess the local prototype beyond a small demonstration.

## II. Data and Pipeline Design

### A. Data Sources

The project combines Bangladesh ADM2 boundaries for all 64 districts,
population centroids, cached Open-Meteo observations, 687 synthetic facilities,
synthetic encounters, stock snapshots, and retained ground-truth outbreak
labels. Weather coverage consists of 64 district files with 1,826 daily
observations each. At scale factor 0.25, encounters contain 6,037,903 records
stored in 60 Parquet files.

### B. Bronze–Silver–Gold Processing

Bronze preserves registered raw encounter and stock inputs with ingestion
metadata. Silver cleans types and dates, applies quality rules, quarantines
invalid records, and uses Spark-native salted SHA-256 handling for patient
identifiers. Gold aggregates clean data by district, disease group, and
epidemiological week. The completed outputs are shown below.

| Stage | Result |
|---|---:|
| Bronze encounters | 6,037,903 |
| Silver clean encounters | 5,887,109 |
| Silver quarantined records | 150,794 |
| Gold epidemiological-week records | 83,127 |
| Gold stock-weekly records | 16,704 |

Quality failures are retained rather than silently dropped. The serving layer
exports only aggregate dashboard JSON and simplified district boundaries. It
can be regenerated with `python serving/export.py`, making the published
dashboard snapshot reproducible from analytical outputs.

## III. Analytics Methods

### A. Early Warning

`analytics/detection/ears.py` implements EARS C2 and generates scored alert
records. EARS C2 compares a current observation with a recent baseline and is
used to prioritise human review. The dashboard serves the most recent 2,000
signals with district, disease, observed cases, baseline, and C2 score.

### B. Forecasting

`analytics/forecast/model.py` creates four-week district–disease forecasts
using SARIMA(1,0,0) × (1,0,0,4), including 95% intervals. The completed run
created 1,280 rows: 64 districts × 5 disease groups × 4 horizons. These values
are planning estimates and require rolling back-testing against future observed
weeks before operational adoption.

### C. Optimisation and Seasonality

`analytics/optimise/allocate.py` uses PuLP and CBC to distribute a constrained
national supply budget according to district need. It created 64
recommendations allocating 19,806 units. The seasonal module produced 16,938
records with mean case patterns, risk levels, and preparedness actions by
epidemiological week.

## IV. Dashboard and Deployment

The D3.js dashboard separates information into Overview, Surveillance, Response
Planning, and Alert Review workspaces to reduce cognitive load. It includes
linked district and disease filters, an interactive district map, time series,
forecast intervals, seasonal patterns, allocation bars, and searchable alerts.
A Three.js/WebGL enhancement has a D3 fallback, so an unavailable 3D renderer
cannot block the data views. The static application is deployed at
[https://episcope-m.vercel.app/](https://episcope-m.vercel.app/).

## V. Scalability Evaluation

Spark optimisations include Parquet column selection and adaptive query
execution. Results are listed below. The 0.5 and 1.0 cases are logical-repeat
experiments; they are not independent physically generated full-scale runs.

| Scale factor | Logical records | Measured duration |
|---:|---:|---:|
| 0.25 | 6,037,903 | 10.69 s |
| 0.50 | 12,075,806 | 5.03 s |
| 1.00 | 24,151,612 | 8.36 s |

The non-monotonic timings reflect local Spark warm-up, caching, and logical
repeats. A future study should use cold starts, repeated trials, resource
utilisation measurements, and a physically generated SF=1 dataset.

## VI. Limitations and Conclusion

EpiScope is a technical prototype, not a production public-health system.
Synthetic encounters cannot establish clinical validity. Alerts require review;
forecasts require validation; and allocation does not yet model road travel,
facility capacity, or dynamic local inventory. Production use would require
governance, authenticated access, monitoring, and secure data handling.

Despite these limits, EpiScope provides a reproducible foundation that turns
millions of records into auditable district-week measures, early-warning
signals, forecasts, allocation recommendations, and preparedness guidance.

## Author Contributions

Record the actual work of each group member before submission; do not claim
equal contribution unless it reflects the work performed.

| Contributor | Primary responsibility | Completed contribution | Review responsibility |
|---|---|---|---|
| Author 1 | Data engineering | [Fill in] | [Fill in] |
| Author 2 | Analytics and evaluation | [Fill in] | [Fill in] |
| Author 3 | Dashboard, policy brief, and presentation | [Fill in] | [Fill in] |

## References

[1] World Health Organization, *Early Warning, Alert and Response System
(EWARS): A Trainer's Guide*, Geneva, Switzerland.

[2] R. D. Hutwagner, W. H. Thompson, and G. M. Seeman, “The bioterrorism
preparedness and response early aberration reporting system (EARS),” *Journal
of Urban Health*, vol. 80, no. 2, 2003.

[3] G. E. P. Box, G. M. Jenkins, G. C. Reinsel, and G. M. Ljung, *Time Series
Analysis: Forecasting and Control*, 5th ed. Hoboken, NJ, USA: Wiley, 2015.

[4] Apache Software Foundation, “Apache Spark documentation,” 2026.

[5] COIN-OR, “PuLP: A linear programming toolkit for Python,” 2026.
