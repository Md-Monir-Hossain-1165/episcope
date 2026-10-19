.PHONY: all setup s1 s2 s3 generate pipeline analytics serve clean

# =========================================================
# EpiScope – Makefile
# Run: make all
# =========================================================

PYTHON = python
SF ?= 1
SEED ?= 42

all: setup s1 s3 generate pipeline analytics serve
	@echo "=== EpiScope pipeline complete ==="

setup:
	@echo "--- Installing dependencies ---"
	pip install -r requirements.txt

s1:
	@echo "--- Downloading S1: District boundaries ---"
	$(PYTHON) generator/download_s1.py

s3:
	@echo "--- Fetching S3: Weather data (Open-Meteo) ---"
	$(PYTHON) generator/fetch_s3_weather.py

generate:
	@echo "--- Building S5, S4, S6 synthetic data (SF=$(SF), seed=$(SEED)) ---"
	$(PYTHON) generator/generate_s5_facilities.py --seed $(SEED)
	$(PYTHON) generator/generate_s4_encounters.py --sf $(SF) --seed $(SEED)
	$(PYTHON) generator/generate_s6_stock.py --seed $(SEED)

pipeline:
	@echo "--- Running Bronze -> Silver -> Gold pipeline ---"
	$(PYTHON) pipeline/bronze/ingest.py
	$(PYTHON) pipeline/silver/transform.py
	$(PYTHON) pipeline/gold/mart.py

run-incremental:
	@echo "--- Running incremental load (hidden test drop) ---"
	$(PYTHON) pipeline/silver/incremental.py

analytics:
	@echo "--- Running analytics ---"
	$(PYTHON) analytics/detection/ears.py
	$(PYTHON) analytics/forecast/model.py
	$(PYTHON) analytics/optimise/allocate.py
	$(PYTHON) analytics/calendar/seasonal.py

serve:
	@echo "--- Exporting suppressed JSON to serving layer ---"
	$(PYTHON) serving/export.py

dashboard:
	@echo "--- Starting D3.js dashboard server (http://localhost:8000) ---"
	cd dashboard && $(PYTHON) -m http.server 8000

scale-experiment:
	@echo "--- Scalability experiment (SF=0.25, 0.5, 1) ---"
	$(PYTHON) orchestration/scale_experiment.py

clean:
	@echo "--- Cleaning generated outputs (keeping bronze) ---"
	find pipeline/silver -name "*.parquet" -delete 2>/dev/null || true
	find pipeline/gold -name "*.parquet" -delete 2>/dev/null || true
	find serving -name "*.json" -delete 2>/dev/null || true
