# EpiScope – Project Proposal
**Course:** CSE4345 – Big Data Analytics | **Group:** Solo | **Date:** 2026-10-20

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

### Conflict Resolution Plan
| ID | Conflict | Planned Resolution |
|---|---|---|
| C1 | Sensitivity vs. specificity | Plot ROC-style threshold trade-off curve on ground truth; choose operating point at F1 maximum |
| C2 | Granularity vs. privacy | Enforce small-cell suppression (counts 1–4 replaced with "<5"); report at district (ADM2) level by default |
| C3 | Efficiency vs. equity | Show efficiency–equity curve across 5 budget levels; recommend α=0.3 equity floor as baseline |
| C4 | Freshness vs. cost | Daily batch cadence — justified because MoH decisions are made weekly; streaming is S-tier bonus |
| C5 | Accuracy vs. interpretability | Sarima with lagged weather covariates — interpretable by epidemiologists; document all assumptions |

---

## 3. Planned Architecture & Tools

| Layer | Tool / Technology |
|---|---|
| Storage | Local Parquet files partitioned by epi-year/epi-week (S-tier: MinIO) |
| Processing | Apache Spark 3.5 (PySpark) |
| Orchestration | Python Makefile / shell scripts |
| Outbreak detection | EARS C2 algorithm; Spark window functions |
| Forecasting | SARIMA with Open-Meteo lagged weather covariates |
| Optimiser | PuLP + CBC solver (linear programme per PDF Section 6.3) |
| Dashboard | D3.js v7 (choropleth, time series, heatmap, allocation, alert table) |
| Quality | Custom PySpark quality rules (12+ rules) + quarantine table |
| Privacy | SHA-256 salted hash for patient_id; small-cell suppression in gold |

---

## 4. Chosen Diseases and Methods
**Diseases in scope:** Dengue (A90/A91), Diarrhoeal disease/Cholera (A00/A09), Influenza-Like Illness (J10/J11), Malaria (B50–B54)
**Detection:** EARS C2 (primary), CUSUM (comparison — satisfies AN-3)
**Forecasting:** SARIMA with exogenous weather variables (precipitation_sum, temperature_2m_max, lag 4–8 weeks for dengue)

---

## 5. Role Assignment (Solo)
All roles (Data Engineer, Analytics Engineer, Visualisation Engineer) are performed by a single student.

---

## 6. Risk Register

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | SF=1 generator too slow (>45 min) | High | High | Tune to Parquet batches; run at SF=0.25 first to validate |
| R2 | Open-Meteo API rate limit or outage | Low | Medium | Cache all responses in bronze immediately; retry with backoff |
| R3 | PySpark memory issues on 16GB laptop | Medium | High | Use local[*] mode; tune spark.driver.memory; use Parquet columnar pushdown |
| R4 | D3.js choropleth complexity | Medium | Medium | Start with a working static map; add linked interaction iteratively |
| R5 | Hidden test drop breaks pipeline | Medium | High | Never hardcode dates, filenames, or row counts; schema-driven ingestion |
| R6 | Solo workload too large for 14 days | High | High | Prioritise all M requirements; implement S/C requirements only if time permits |
| R7 | geoBoundaries/HDX download unavailable | Low | Low | Use cached GeoJSON; alternative HDX source documented in download script |
