"""Export compact, privacy-safe dashboard JSON from Gold and analytics outputs."""
import json
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "serving" / "data"
DASHBOARD_OUT = ROOT / "dashboard" / "data"

def write(name, frame):
    OUT.mkdir(parents=True, exist_ok=True)
    DASHBOARD_OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / name
    payload = frame.to_json(orient="records", date_format="iso")
    path.write_text(payload, encoding="utf-8")
    (DASHBOARD_OUT / name).write_text(payload, encoding="utf-8")
    print(f"[OK] {len(frame):,} rows, {path.stat().st_size / 1e6:.2f} MB -> {path}")

def main():
    epi = pd.read_parquet(ROOT / "pipeline/gold/epi_weekly")
    epi["case_count"] = epi.case_count.mask(epi.case_count < 0)
    epi["week_start"] = pd.to_datetime(epi.epi_year.astype(str)+epi.epi_week.astype(str).str.zfill(2)+"-1", format="%G%V-%u")
    # Keep two years for responsive browser delivery; individual small cells remain null.
    cutoff = epi.week_start.max() - pd.Timedelta(weeks=104)
    write("epi_weekly.json", epi.loc[epi.week_start >= cutoff, ["district","division","disease_group","epi_year","epi_week","week_start","case_count","incidence_per_100k","admitted_count","death_count"]])
    alerts = pd.read_parquet(ROOT / "analytics/detection/alerts.parquet")
    write("alerts.json", alerts.loc[alerts.ears_c2_alert, ["district","division","disease_group","epi_year","epi_week","week_start","observed_cases","baseline_mean","c2_score","alert_level"]].tail(2000))
    write("forecasts.json", pd.read_parquet(ROOT / "analytics/forecast/forecasts.parquet"))
    write("allocation.json", pd.read_parquet(ROOT / "analytics/optimise/allocation.parquet"))
    cal = pd.read_parquet(ROOT / "analytics/calendar/preparedness_calendar.parquet")
    write("calendar.json", cal[["district","division","disease_group","epi_week","cases","risk_level","recommended_action"]])
    # Topology-preserving simplification retains valid district shapes without
    # the artefacts caused by naïvely sampling coordinate rings.
    from shapely.geometry import shape, mapping
    boundaries = json.loads((ROOT / "pipeline/bronze/boundaries/bangladesh_adm2.geojson").read_text(encoding="utf-8"))
    for feature in boundaries.get("features", []):
        geom = feature.get("geometry") or {}
        if geom:
            feature["geometry"] = mapping(shape(geom).simplify(0.008, preserve_topology=True))
    geo_path = OUT / "bangladesh_districts.geojson"
    geo_path.write_text(json.dumps(boundaries, separators=(",", ":")), encoding="utf-8")
    DASHBOARD_OUT.mkdir(parents=True, exist_ok=True)
    (DASHBOARD_OUT / geo_path.name).write_text(geo_path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"[OK] simplified boundary geometry, {geo_path.stat().st_size / 1e6:.2f} MB -> {geo_path}")
    print("[DONE] Serving exports complete")

if __name__ == "__main__": main()
