"""
S3 Weather Fetcher – Open-Meteo Historical Archive API
Fetches daily weather for all 64 Bangladesh districts (2021-2025)
Caches raw JSON in pipeline/bronze/weather/ (required by ING-1)

Usage:
    python generator/fetch_s3_weather.py

Requirements:
    pip install requests
"""

import requests
import json
import time
import csv
from pathlib import Path

BRONZE_WEATHER_DIR = Path("pipeline/bronze/weather")
BRONZE_WEATHER_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

WEATHER_PARAMS = {
    "start_date": "2021-01-01",
    "end_date":   "2025-12-31",
    "daily": ",".join([
        "precipitation_sum",
        "temperature_2m_max",
        "temperature_2m_min",
        "relative_humidity_2m_max",
        "relative_humidity_2m_min",
        "windspeed_10m_max",
    ]),
    "timezone": "Asia/Dhaka"
}

def load_districts():
    """Load district centroids from the S2 population CSV."""
    districts = []
    s2_csv = Path("pipeline/bronze/population/bd_districts_population.csv")
    with open(s2_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            districts.append({
                "name":      row["district"],
                "division":  row["division"],
                "lat":  float(row["centroid_lat"]),
                "lon":  float(row["centroid_lon"]),
            })
    return districts

def fetch_district(district: dict, max_retries: int = 5) -> bool:
    name_safe = district["name"].lower().replace("'", "").replace(" ", "_")
    out_file = BRONZE_WEATHER_DIR / f"{name_safe}.json"

    if out_file.exists():
        print(f"  [SKIP] {district['name']} - already cached")
        return True

    params = {
        **WEATHER_PARAMS,
        "latitude":  district["lat"],
        "longitude": district["lon"],
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)

            if resp.status_code == 429:
                wait = 2 ** attempt * 5  # 10s, 20s, 40s, 80s, 160s
                print(f"  [WAIT] {district['name']} - rate limited, retrying in {wait}s (attempt {attempt}/{max_retries})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            # Add metadata before saving to bronze (ING-1: ingestion metadata)
            data["_meta"] = {
                "district":  district["name"],
                "division":  district["division"],
                "latitude":  district["lat"],
                "longitude": district["lon"],
                "source":    "open-meteo.com",
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            days = len(data.get("daily", {}).get("time", []))
            print(f"  [OK]   {district['name']} - {days} days saved")
            return True

        except Exception as e:
            if attempt == max_retries:
                print(f"  [FAIL] {district['name']} - giving up after {max_retries} attempts: {e}")
                return False
            wait = 2 ** attempt * 3
            print(f"  [ERR]  {district['name']} - {e}, retrying in {wait}s")
            time.sleep(wait)

def main():
    print("=== S3 Weather Fetcher ===")
    districts = load_districts()
    print(f"Fetching weather for {len(districts)} districts (2021-2025)...\n")

    ok, skip, fail = 0, 0, 0
    for i, d in enumerate(districts, 1):
        print(f"[{i:02d}/{len(districts)}]", end=" ")
        result = fetch_district(d)
        if result:
            if (BRONZE_WEATHER_DIR / f"{d['name'].lower().replace(' ', '_')}.json").exists():
                ok += 1
        else:
            fail += 1
        # Respect Open-Meteo rate limits: ~0.5s between requests
        time.sleep(0.6)

    print(f"\n=== Done ===")
    print(f"  Fetched : {ok}")
    print(f"  Failed  : {fail}")
    print(f"  Files in: {BRONZE_WEATHER_DIR}")

if __name__ == "__main__":
    main()
