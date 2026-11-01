# EpiScope – Project Proposal

**Course:** CSE4345 – Big Data Analytics
**Group:** 1 — TriScope
**Instructor:** Rokan Uddin Faruqui
**Members:**
- Riduanul Abedin Shihab — 0222220005101164 (Data Engineering Lead)
- Md Monir Hossain — 0222220005101165 (Dashboard and Serving Lead)
- Syed Intiqab Ahmed Rohan — 0222220005101167 (Analytics and Evaluation Lead)

**Date:** 2026-10-20
**Repository:** https://github.com/Md-Monir-Hossain-1165/episcope
**Dashboard:** https://episcope-m.vercel.app

---

## 1. Problem Restatement
Bangladesh faces a heavy seasonal burden of infectious disease. Dengue follows the monsoon, cholera/diarrhoea rise before the monsoon and after floods, influenza peaks in the rainy season, and malaria is concentrated in the Chittagong Hill Tracts. The Ministry of Health (MoH) needs a working prototype of a national data platform that turns routine health, climate and logistics data into early warnings and an optimal response plan across all 64 districts.

**EpiScope** will:
1. Detect unusual rises in dengue, diarrhoeal disease, ILI and malaria as early as possible
2. Forecast expected cases and hospital demand for the next 1–4 weeks
3. Recommend optimal allocation of a limited national stock across districts
4. Produce a seasonal preparedness calendar for the year ahead

---

## 2. Stakeholder & Conflict Analysis

### Stakeholders
| Stakeholder | Primary Need | Concern |
|---|---|---|
| MoH policymakers | National situation at a glance; defensible allocation | "Why did district X get more than Y?" |
| Surveillance epidemiologists | Reliable signals, method transparency | Reporting artefacts treated as outbreaks |
| District health officers | Early local warning; own district forecast | Alert fatigue; delayed supplies |
| Hospital managers | Bed and consumable demand forecasts | Forecasts without uncertainty bands |
| City vector-control units | Hotspots and timing for larval-control campaigns | Map at too coarse a resolution |
| Patients and public | Timely response; no misuse of personal data | Re-identification; stigma |
| Privacy regulator | Minimal data, purpose limitation, suppression of small counts | Line-level data leaving secure zones |

### Conflict Resolution Plan (planned → final decision)
| ID | Conflict | Planned Resolution | Final Decision |
|---|---|---|---|
| C1 | Sensitivity vs. specificity | Plot ROC-style threshold trade-off curve on ground truth; choose an operating point | Threshold sweep over 76,535 scored weeks against withheld labels; high alert at z ≥ 3, watch band at z > 2 (87% of outbreaks flagged, 6.3% alert burden) |
| C2 | Granularity vs. privacy | Enforce small-cell suppression (counts 1–4 hidden); report at district (ADM2) level | Implemented: district-level only; counts 1–4 replaced with a sentinel before publication; aggregate-only serving |
| C3 | Efficiency vs. equity | Show efficiency–equity curve across 5 budget levels; recommend an equity floor | Implemented: LP vs. population-proportional baseline (19,806 vs. 19,423 units met), efficiency–equity curve at 40–80% of need; LP reported as efficiency tool with a fairness caveat |
| C4 | Freshness vs. cost | Daily batch cadence; streaming as bonus | Weekly batch cadence — matches the weekly review meetings the dashboard supports; incremental monthly loading for late files |
| C5 | Accuracy vs. interpretability | SARIMA with lagged weather covariates | Fixed-order SARIMA(1,0,0)×(1,0,0,4) with intervals, evaluated by rolling backtest; weather covariates omitted for tractability and interpretability (documented trade-off) |

---

## 3. Planned Architecture & Tools

| Layer | Tool / Technology |
|---|---|
| Storage | Local Parquet files; Silver partitioned by division/epi-year/epi-week, Gold by year |
| Processing | Apache Spark 3.5 (PySpark), broadcast join for the 64-row district mapping |
| Orchestration | Makefile targets; incremental monthly loader; per-run quality report |
| Outbreak detection | EARS C2 (weekly adaptation) with CUSUM comparison; Spark window functions |
| Forecasting | SARIMA with 95% intervals; rolling-origin backtest vs. seasonal-naive |
| Optimiser | PuLP + CBC solver (linear programme with a minimum-service floor) |
| Dashboard | D3.js v7 (choropleth, time series, heatmap, allocation, alert table) |
| Quality | Custom PySpark rules (12 controls: 11 tag-based checks + de-duplication) + retained quarantine |
| Privacy | Salted SHA-256 for patient identifiers; small-cell suppression in Gold; aggregate-only export |

---

## 4. Chosen Diseases and Methods
**Diseases in scope:** Dengue (A90/A91), Diarrhoeal disease/Cholera (A00/A09), Influenza-Like Illness (J10/J11), Malaria (B50–B54), plus an other/unclassified group
**Detection:** EARS C2 (primary), CUSUM (comparison — satisfies AN-3)
**Forecasting:** SARIMA(1,0,0)×(1,0,0,4) with constant, horizons 1–4 weeks, 95% intervals

---

## 5. Role Assignment (Three-Person Team)
| Team member | Primary role | Responsibility |
|---|---|---|
| Riduanul Abedin Shihab — 0222220005101164 | Data Engineer | Sources, synthetic generator, Spark pipeline, quality, privacy, and reproducibility |
| Md Monir Hossain — 0222220005101165 | Visualisation Engineer | Serving layer, dashboard, responsive and usability testing, policy brief, and video |
| Syed Intiqab Ahmed Rohan — 0222220005101167 | Analytics Engineer | Detection, forecasting and backtest, optimisation, calendar, and evaluation |

All members review the final report, claims, and presentation.

---

## 6. Risk Register

| # | Risk | Likelihood | Impact | Mitigation | Outcome |
|---|---|---|---|---|---|
| R1 | SF=1 generator too slow (>45 min) | High | High | Tune to Parquet batches; run at SF=0.25 first to validate | Held: SF=0.25 used throughout; scale test used logical repeats |
| R2 | Open-Meteo API rate limit or outage | Low | Medium | Cache all responses in bronze immediately; retry with backoff | Held: weather cached (64 × 1,826 daily observations) |
| R3 | PySpark memory issues on 16GB laptop | Medium | High | Use local[*] mode; tune spark.driver.memory; Parquet columnar pushdown | Held: local[*], 4 GB driver, 16 shuffle partitions |
| R4 | D3.js choropleth complexity | Medium | Medium | Start with a working static map; add linked interaction iteratively | Held: SVG choropleth with linked filters |
| R5 | Hidden test drop breaks pipeline | Medium | High | Never hardcode dates, filenames, or row counts; schema-driven ingestion | Held: monthly-named files, idempotent partition overwrite |
| R6 | Workload too large for the project window | High | High | Prioritise all M requirements; implement S/C requirements only if time permits | Partly held: VIS-5 what-if panel not delivered (documented limitation) |
| R7 | geoBoundaries/HDX download unavailable | Low | Low | Use cached GeoJSON; alternative source documented in download script | Held: boundary cache + simplified geometry |
