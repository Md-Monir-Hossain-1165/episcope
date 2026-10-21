"""
S6 Generator – Daily Stock & Bed-Occupancy Snapshots
Produces one JSON record per facility per day (incremental feed).
Output: pipeline/bronze/stock/stock_YYYY_MM.parquet

Usage:
    python generator/generate_s6_stock.py --seed 42
"""

import argparse
import json
import random
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, timedelta

OUTPUT_DIR = Path("pipeline/bronze/stock")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SIM_START = date(2021, 1, 1)
SIM_END   = date(2025, 12, 31)


def generate_month(year: int, month: int, facilities: pd.DataFrame, seed: int) -> pd.DataFrame:
    rng = random.Random(seed * 10000 + year * 100 + month)
    rng_np = np.random.default_rng(seed * 10000 + year * 100 + month)

    first_day = date(year, month, 1)
    last_day  = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    days = [(first_day + timedelta(days=i)) for i in range((last_day - first_day).days + 1)]

    rows = []
    import time as _time
    for d in days:
        for _, fac in facilities.iterrows():
            beds      = int(fac["total_beds"])
            icu_beds  = int(fac["icu_beds"])

            # Bed occupancy: 40-90%, higher in urban large hospitals
            base_occ = 0.55 if fac["facility_type"] == "district_hospital" else 0.35
            occ_rate = min(0.95, max(0.0, rng.gauss(base_occ, 0.12)))
            occupied_beds = min(int(beds * occ_rate), beds)
            occupied_icu  = min(int(icu_beds * rng.uniform(0.3, 0.9)), icu_beds)

            # Stock levels: uniform random, some facilities periodically run low
            saline_kits = max(0, int(rng_np.normal(200, 60)))
            ns1_kits    = max(0, int(rng_np.normal(80, 30)))
            ors_packets = max(0, int(rng_np.normal(500, 120)))
            antimalarials = max(0, int(rng_np.normal(60, 25)))

            # Simulate occasional stockouts (~5% chance per item)
            if rng.random() < 0.05: saline_kits = 0
            if rng.random() < 0.05: ns1_kits = 0
            if rng.random() < 0.05: ors_packets = 0

            rows.append({
                "facility_id":     fac["facility_id"],
                "district":        fac["district"],
                "snapshot_date":   d.isoformat(),
                "occupied_beds":   occupied_beds,
                "total_beds":      beds,
                "occupied_icu":    occupied_icu,
                "total_icu":       icu_beds,
                "iv_saline_units": saline_kits,
                "ns1_kits":        ns1_kits,
                "ors_packets":     ors_packets,
                "antimalarials":   antimalarials,
                "_batch_id":       f"stock_{year}_{month:02d}",
                "_loaded_at":      _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime()),
            })

    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    fac_csv = Path("pipeline/bronze/facilities/facility_registry.csv")
    facilities = pd.read_csv(fac_csv)
    # Only active facilities
    facilities = facilities[facilities["active"] == 1].reset_index(drop=True)
    print(f"Generating S6 stock snapshots for {len(facilities)} active facilities...")

    total = 0
    for year in range(2021, 2026):
        for month in range(1, 13):
            out_file = OUTPUT_DIR / f"stock_{year}_{month:02d}.parquet"
            if out_file.exists():
                print(f"  [SKIP] {year}-{month:02d}")
                continue
            df = generate_month(year, month, facilities, args.seed)
            df.to_parquet(out_file, index=False)
            total += len(df)
            print(f"  [OK] {year}-{month:02d}: {len(df):,} rows")

    print(f"\n[DONE] Total stock records: {total:,} -> {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
