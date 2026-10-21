"""
S4 Generator – Synthetic Health Facility Encounter Linelist
Produces 25M+ records (2021-2025) across 64 Bangladesh districts.
Epidemiologically realistic: seasonality, climate drivers, overdispersion,
30+ injected outbreaks, reporting delays, duplicates, artefacts.

Usage:
    python generator/generate_s4_encounters.py --sf 0.25 --seed 42

Output:
    pipeline/bronze/encounters/encounters_YYYY_MM.parquet  (partitioned)
    generator/ground_truth_outbreaks.csv                   (NEVER read by detection code)
"""

import argparse
import hashlib
import math
import random
import json
import uuid
from pathlib import Path
from datetime import date, timedelta
import pandas as pd
import numpy as np

# ── Output paths ──────────────────────────────────────────────────────────────
BRONZE_ENC_DIR = Path("pipeline/bronze/encounters")
BRONZE_ENC_DIR.mkdir(parents=True, exist_ok=True)
GROUND_TRUTH_PATH = Path("generator/ground_truth_outbreaks.csv")

# ── Simulation window ─────────────────────────────────────────────────────────
SIM_START = date(2021, 1, 1)
SIM_END   = date(2025, 12, 31)
DATES     = [SIM_START + timedelta(days=i)
             for i in range((SIM_END - SIM_START).days + 1)]

# ── Disease codes (ICD-10) ────────────────────────────────────────────────────
DISEASES = {
    "dengue":    {"codes": ["A90", "A91"],         "base_rate": 7.0},
    "diarrhoea": {"codes": ["A00", "A01", "A09"],  "base_rate": 14.0},
    "ili":       {"codes": ["J10", "J11"],         "base_rate": 11.0},
    "malaria":   {"codes": ["B50", "B51", "B54"],  "base_rate": 3.5},
    "other":     {"codes": ["J00", "K30", "N39"],  "base_rate": 22.0},
}

BACKGROUND_CODES = ["J00","K30","N39","R50","M79","Z00"]


def load_districts():
    pop_csv = Path("pipeline/bronze/population/bd_districts_population.csv")
    df = pd.read_csv(pop_csv)
    return df.to_dict("records")


def load_facilities():
    fac_csv = Path("pipeline/bronze/facilities/facility_registry.csv")
    df = pd.read_csv(fac_csv)
    return df.groupby("district")["facility_id"].apply(list).to_dict()


def load_weather():
    """Load cached Open-Meteo weather JSON into dict: district -> date -> {precip, tmax}."""
    weather_dir = Path("pipeline/bronze/weather")
    out = {}
    for jf in weather_dir.glob("*.json"):
        data = json.loads(jf.read_text(encoding="utf-8"))
        district = data["_meta"]["district"]
        daily = data.get("daily", {})
        times = daily.get("time", [])
        precip = daily.get("precipitation_sum", [None]*len(times))
        tmax   = daily.get("temperature_2m_max", [None]*len(times))
        out[district] = {t: {"precip": p, "tmax": tx}
                         for t, p, tx in zip(times, precip, tmax)}
    return out


def seasonal_multiplier(disease: str, d: date, district: str, rng: random.Random) -> float:
    """Return a seasonal scaling factor for the given disease, date, district."""
    doy = d.timetuple().tm_yday
    if disease == "dengue":
        # Peak July-October (monsoon); stronger in urban districts
        peak = 220  # ~Aug 8
        amp = 3.5 if "Dhaka" in district or "Chittagong" in district else 1.8
        return 1.0 + amp * math.exp(-0.5 * ((doy - peak) / 45) ** 2)
    elif disease == "diarrhoea":
        # Two peaks: pre-monsoon (Apr) and post-flood (Sep)
        p1 = 105; p2 = 265
        return 1.0 + 1.8 * math.exp(-0.5*((doy-p1)/30)**2) \
                   + 1.2 * math.exp(-0.5*((doy-p2)/30)**2)
    elif disease == "ili":
        # Winter peak (Dec-Jan) and mild monsoon peak
        p1 = 10; p2 = 200
        return 1.0 + 2.0 * math.exp(-0.5*((doy-p1)/35)**2) \
                   + 0.8 * math.exp(-0.5*((doy-p2)/30)**2)
    elif disease == "malaria":
        # Peak Aug; mostly hill tracts (Rangamati, Bandarban, Khagrachhari)
        hill = district in ("Rangamati", "Bandarban", "Khagrachhari")
        amp = 8.0 if hill else 0.3
        return 1.0 + amp * math.exp(-0.5 * ((doy - 225) / 50) ** 2)
    return 1.0


def climate_lag_multiplier(disease: str, district: str, d: date, weather: dict) -> float:
    """
    Dengue incidence responds to rainfall ~4-8 weeks prior.
    Diarrhoea spikes after extreme rainfall 1-2 weeks prior.
    """
    w = weather.get(district, {})
    if not w:
        return 1.0

    if disease == "dengue":
        lag_days = 42  # ~6 weeks
        lag_date = (d - timedelta(days=lag_days)).isoformat()
        wd = w.get(lag_date, {})
        precip = wd.get("precip") or 0
        return 1.0 + min(precip / 25.0, 2.5)  # cap at 3.5x

    elif disease == "diarrhoea":
        lag_days = 7
        lag_date = (d - timedelta(days=lag_days)).isoformat()
        wd = w.get(lag_date, {})
        precip = wd.get("precip") or 0
        return 1.0 + min(precip / 40.0, 2.0) if precip > 30 else 1.0

    return 1.0


def define_outbreaks(rng: random.Random, districts: list) -> list:
    """Define 30+ outbreak events. Saved to ground_truth_outbreaks.csv."""
    outbreaks = []
    district_names = [d["district"] for d in districts]

    specs = [
        # (disease, size_cat, n)
        ("dengue",    "large",   5),
        ("dengue",    "medium",  8),
        ("dengue",    "small",   4),
        ("diarrhoea", "large",   3),
        ("diarrhoea", "medium",  4),
        ("ili",       "medium",  3),
        ("malaria",   "small",   2),
        # Special: slow-growing dengue
        ("dengue",    "slow",    1),
        # Special: nationwide novel respiratory surge (ILI-like)
        ("ili",       "national",1),
    ]

    ob_id = 1
    for disease, size_cat, count in specs:
        for _ in range(count):
            dist = rng.choice(district_names)
            start_offset = rng.randint(0, (SIM_END - SIM_START).days - 120)
            start_d = SIM_START + timedelta(days=start_offset)

            if size_cat == "large":
                duration = rng.randint(35, 70)
                peak_mult = rng.uniform(6.0, 12.0)
            elif size_cat == "medium":
                duration = rng.randint(21, 40)
                peak_mult = rng.uniform(3.0, 6.0)
            elif size_cat == "small":
                duration = rng.randint(10, 25)
                peak_mult = rng.uniform(1.8, 3.5)
            elif size_cat == "slow":
                duration = rng.randint(60, 90)
                peak_mult = rng.uniform(2.0, 4.0)
            else:  # national
                dist = "ALL"
                duration = rng.randint(30, 60)
                peak_mult = rng.uniform(3.0, 6.0)

            end_d = start_d + timedelta(days=duration)
            outbreaks.append({
                "outbreak_id": f"OB{ob_id:03d}",
                "disease":     disease,
                "district":    dist,
                "start_date":  start_d.isoformat(),
                "end_date":    min(end_d, SIM_END).isoformat(),
                "duration_days": duration,
                "peak_multiplier": round(peak_mult, 2),
                "size_category": size_cat,
            })
            ob_id += 1

    # Reporting artefact: a facility goes silent for 3 weeks then back-fills
    # (stored separately so detection code can be tested against it)
    outbreaks.append({
        "outbreak_id": "ARTEFACT_01",
        "disease":     "dengue",
        "district":    "Comilla",
        "start_date":  "2023-06-01",
        "end_date":    "2023-06-21",
        "duration_days": 21,
        "peak_multiplier": 0.0,  # 0 = silence, not real outbreak
        "size_category": "artefact",
    })

    return outbreaks


def outbreak_multiplier(outbreaks: list, disease: str, district: str, d: date) -> float:
    """Return the outbreak boost factor for this district/disease/date."""
    mult = 1.0
    d_str = d.isoformat()
    for ob in outbreaks:
        if ob["disease"] != disease:
            continue
        if ob["district"] != district and ob["district"] != "ALL":
            continue
        if ob["size_category"] == "artefact":
            continue
        if ob["start_date"] <= d_str <= ob["end_date"]:
            # Gaussian bell around midpoint of outbreak
            ob_start = date.fromisoformat(ob["start_date"])
            ob_end   = date.fromisoformat(ob["end_date"])
            mid = ob_start + (ob_end - ob_start) / 2
            days_from_mid = abs((d - mid).days)
            sigma = max((ob_end - ob_start).days / 4, 5)
            boost = ob["peak_multiplier"] * math.exp(-0.5 * (days_from_mid / sigma) ** 2)
            mult += boost
    return max(mult, 0.0)


def neg_binom_sample(rng_np, mu: float, dispersion: float = 0.3) -> int:
    """Sample from negative-binomial with mean mu and dispersion k."""
    if mu <= 0:
        return 0
    # NB parameterisation: n=k, p=k/(k+mu)
    k = dispersion
    p = k / (k + mu)
    return int(rng_np.negative_binomial(k, p))


def make_patient_id(rng: random.Random, district: str) -> str:
    """Generate a synthetic national ID (bronze only)."""
    return f"PID{district[:3].upper()}{rng.randint(100000000, 999999999)}"


def reporting_delay(rng: random.Random, onset: date) -> date:
    """Right-skewed delay 0-21 days; fewer reports on weekends."""
    delay = int(rng.betavariate(1.5, 5) * 21)
    report = onset + timedelta(days=delay)
    # Shift weekend reports to Monday
    if report.weekday() == 5:
        report += timedelta(days=2)
    elif report.weekday() == 6:
        report += timedelta(days=1)
    return report


def generate_month(
    year: int, month: int,
    districts: list, fac_by_district: dict,
    weather: dict, outbreaks: list,
    sf: float, seed: int
) -> pd.DataFrame:
    """Generate all encounter records for one calendar month."""
    rng = random.Random(seed * 10000 + year * 100 + month)
    rng_np = np.random.default_rng(seed * 10000 + year * 100 + month)

    # Days in this month
    first_day = date(year, month, 1)
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    month_dates = [first_day + timedelta(days=i)
                   for i in range((last_day - first_day).days + 1)]

    rows = []

    for d in month_dates:
        for dist_row in districts:
            dist = dist_row["district"]
            pop = dist_row["population_2022"]
            fac_ids = fac_by_district.get(dist, ["FAC00001"])

            for disease, info in DISEASES.items():
                base = info["base_rate"] * (pop / 1_000_000) * sf
                sm   = seasonal_multiplier(disease, d, dist, rng)
                cm   = climate_lag_multiplier(disease, dist, d, weather)
                om   = outbreak_multiplier(outbreaks, disease, dist, d)
                mu   = base * sm * cm * om

                count = neg_binom_sample(rng_np, mu, dispersion=0.35)
                if count == 0:
                    continue

                for _ in range(count):
                    onset = d
                    report = reporting_delay(rng, onset)

                    # ICD-10 code
                    code = rng.choice(info["codes"])

                    # Age/sex consistent with S2 structure
                    age = int(rng.betavariate(2, 5) * 80)
                    if rng.random() < 0.01:
                        age = rng.randint(100, 212)  # ~1% invalid
                    sex = rng.choice(["M", "F"])

                    # Lab confirmed, admitted, outcome
                    lab_prob = 0.35 if disease in ("dengue", "malaria") else 0.15
                    lab_confirmed = int(rng.random() < lab_prob)
                    adm_prob = 0.1 * om  # higher during outbreaks
                    admitted = int(rng.random() < min(adm_prob, 0.6))
                    died = int(admitted and rng.random() < 0.02)

                    fac = rng.choice(fac_ids)
                    # ~0.5% orphan facility keys
                    if rng.random() < 0.005:
                        fac = f"FAC{rng.randint(99000, 99999):05d}"

                    pid = make_patient_id(rng, dist)

                    rows.append({
                        "encounter_id":  str(uuid.UUID(int=rng.getrandbits(128))),
                        "patient_id":    pid,
                        "facility_id":   fac,
                        "district":      dist,
                        "onset_date":    onset.isoformat(),
                        "report_date":   min(report, last_day).isoformat(),
                        "age_years":     age,
                        "sex":           sex,
                        "icd10_code":    code,
                        "disease_group": disease,
                        "lab_confirmed": lab_confirmed,
                        "admitted":      admitted,
                        "outcome":       "died" if died else ("discharged" if admitted else "outpatient"),
                    })

    df = pd.DataFrame(rows)

    # Inject ~2% deliberate duplicates (resubmitted records)
    if len(df) > 0:
        n_dup = max(1, int(len(df) * 0.02))
        dups = df.sample(n=min(n_dup, len(df)), random_state=seed+month).copy()
        df = pd.concat([df, dups], ignore_index=True)

    return df


def main():
    parser = argparse.ArgumentParser(description="Generate S4 encounter linelist")
    parser.add_argument("--sf",   type=float, default=1.0,  help="Scale factor (0.25, 0.5, 1)")
    parser.add_argument("--seed", type=int,   default=42,   help="Random seed")
    args = parser.parse_args()

    sf   = args.sf
    seed = args.seed

    print(f"=== S4 Encounter Generator (SF={sf}, seed={seed}) ===")

    districts = load_districts()
    fac_by_district = load_facilities()
    weather = load_weather()

    # Define and save ground-truth outbreaks (detection code must NEVER read this)
    rng_ob = random.Random(seed)
    outbreaks = define_outbreaks(rng_ob, districts)
    ob_df = pd.DataFrame(outbreaks)
    ob_df.to_csv(GROUND_TRUTH_PATH, index=False)
    print(f"[OK] {len(outbreaks)} ground-truth outbreaks -> {GROUND_TRUTH_PATH}")
    print("     (Detection code must NEVER read this file!)")

    # Generate month by month
    total_rows = 0
    for year in range(2021, 2026):
        for month in range(1, 13):
            out_file = BRONZE_ENC_DIR / f"encounters_{year}_{month:02d}.parquet"
            if out_file.exists():
                existing = pd.read_parquet(out_file)
                print(f"  [SKIP] {year}-{month:02d} already exists ({len(existing):,} rows)")
                total_rows += len(existing)
                continue

            df = generate_month(year, month, districts, fac_by_district,
                                weather, outbreaks, sf, seed)

            # Add ingestion metadata (ING-1)
            import time as _time
            df["_batch_id"]    = f"batch_{year}_{month:02d}_sf{sf}"
            df["_loaded_at"]   = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
            df["_source"]      = "S4_synthetic_generator"

            df.to_parquet(out_file, index=False)
            print(f"  [OK] {year}-{month:02d}: {len(df):,} rows -> {out_file.name}")
            total_rows += len(df)

    print(f"\n=== Complete ===")
    print(f"Total records : {total_rows:,}")
    print(f"Target (SF=1) : ~25,000,000")
    print(f"Output dir    : {BRONZE_ENC_DIR}")


if __name__ == "__main__":
    main()
