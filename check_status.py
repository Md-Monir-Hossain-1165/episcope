from pathlib import Path
base = Path("E:/BDA_complex/episcope")

items = [
    ("S1 GeoJSON", base/"pipeline/bronze/boundaries/bangladesh_adm2.geojson"),
    ("S2 Population CSV", base/"pipeline/bronze/population/bd_districts_population.csv"),
    ("S3 Weather dhaka.json", base/"pipeline/bronze/weather/dhaka.json"),
    ("S5 Facility registry", base/"pipeline/bronze/facilities/facility_registry.csv"),
    ("Ground truth outbreaks", base/"generator/ground_truth_outbreaks.csv"),
    ("Bronze ingest.py", base/"pipeline/bronze/ingest.py"),
    ("Silver transform.py", base/"pipeline/silver/transform.py"),
    ("Silver incremental.py", base/"pipeline/silver/incremental.py"),
    ("Gold mart.py", base/"pipeline/gold/mart.py"),
    ("Quality rules.py", base/"pipeline/quality/rules.py"),
    ("Makefile", base/"Makefile"),
    ("proposal.md", base/"docs/proposal.md"),
    ("AI_USAGE.md", base/"AI_USAGE.md"),
]
for name, path in items:
    status = "OK" if path.exists() else "MISSING"
    print(f"  {status:<8} {name}")

enc = list((base/"pipeline/bronze/encounters").glob("*.parquet"))
print(f"  OK       S4 Encounter files: {len(enc)} parquet files")

wx = list((base/"pipeline/bronze/weather").glob("*.json"))
print(f"  OK       S3 Weather files: {len(wx)}/64")

st = list((base/"pipeline/bronze/stock").glob("*.parquet"))
print(f"  OK       S6 Stock files: {len(st)} parquet files")

silver = base/"pipeline/silver/encounters"
gold   = base/"pipeline/gold/epi_weekly"
print(f"  {'OK' if silver.exists() else 'MISSING':<8} Silver encounters layer")
print(f"  {'OK' if gold.exists() else 'MISSING':<8} Gold epi_weekly mart")

for f in ["analytics/detection/ears.py","analytics/forecast/model.py",
          "analytics/optimise/allocate.py","analytics/calendar/seasonal.py","dashboard/index.html"]:
    p = base/f
    print(f"  {'OK' if p.exists() else 'MISSING':<8} {f}")
