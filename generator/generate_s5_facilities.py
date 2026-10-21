"""
S5 Generator – Synthetic Facility Registry
Produces 600+ health facilities across all 64 Bangladesh districts.
Output: pipeline/bronze/facilities/facility_registry.csv

Usage:
    python generator/generate_s5_facilities.py --seed 42
"""

import argparse
import csv
import random
import math
from pathlib import Path

OUTPUT_DIR = Path("pipeline/bronze/facilities")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# District population data (for proportional facility allocation)
DISTRICTS = [
    ("Dhaka","Dhaka",23.8103,90.4125,14543124),
    ("Gazipur","Dhaka",23.9994,90.4122,4447560),
    ("Narayanganj","Dhaka",23.6238,90.4995,2937948),
    ("Narsingdi","Dhaka",23.9282,90.7156,2524681),
    ("Manikganj","Dhaka",23.8629,90.0140,1520282),
    ("Munshiganj","Dhaka",23.5422,90.5310,1522357),
    ("Rajbari","Dhaka",23.7574,89.6440,1136218),
    ("Shariatpur","Dhaka",23.2423,90.4349,1219811),
    ("Madaripur","Dhaka",23.1638,90.1994,1181577),
    ("Gopalganj","Dhaka",22.8534,89.8272,1227162),
    ("Faridpur","Dhaka",23.6070,89.8429,1928802),
    ("Kishoreganj","Dhaka",24.1844,90.9770,3098000),
    ("Tangail","Dhaka",24.2513,89.9167,3765000),
    ("Jamalpur","Mymensingh",24.9000,89.9478,2629000),
    ("Sherpur","Mymensingh",25.0204,90.0152,1492000),
    ("Mymensingh","Mymensingh",24.7471,90.4203,5600000),
    ("Netrokona","Mymensingh",24.8703,90.7279,2292000),
    ("Chittagong","Chittagong",22.3569,91.7832,8439000),
    ("Cox's Bazar","Chittagong",21.4272,92.0058,2617000),
    ("Feni","Chittagong",23.0153,91.3964,1630000),
    ("Brahmanbaria","Chittagong",23.9608,91.1115,3465000),
    ("Comilla","Chittagong",23.4607,91.1809,6060000),
    ("Chandpur","Chittagong",23.2233,90.6517,2485000),
    ("Lakshmipur","Chittagong",22.9422,90.8289,1900000),
    ("Noakhali","Chittagong",22.8696,91.0996,3739000),
    ("Khagrachhari","Chittagong",23.1193,91.9848,741000),
    ("Rangamati","Chittagong",22.6552,92.1988,659000),
    ("Bandarban","Chittagong",22.1953,92.2184,404000),
    ("Sylhet","Sylhet",24.8949,91.8687,3952000),
    ("Moulvibazar","Sylhet",24.4829,91.7774,2100000),
    ("Habiganj","Sylhet",24.3745,91.4156,2337000),
    ("Sunamganj","Sylhet",25.0658,91.3987,2721000),
    ("Rajshahi","Rajshahi",24.3745,88.6042,2595000),
    ("Natore","Rajshahi",24.4203,89.0000,1840000),
    ("Naogaon","Rajshahi",24.8091,88.9313,3157000),
    ("Chapainawabganj","Rajshahi",24.5967,88.2745,1765000),
    ("Pabna","Rajshahi",24.0064,89.2371,2929000),
    ("Sirajganj","Rajshahi",24.4503,89.7001,3344000),
    ("Bogra","Rajshahi",24.8510,89.3697,3822000),
    ("Joypurhat","Rajshahi",25.1043,89.0231,978000),
    ("Khulna","Khulna",22.8456,89.5403,2318000),
    ("Bagerhat","Khulna",22.6600,89.7897,1604000),
    ("Satkhira","Khulna",22.7185,89.0724,2000000),
    ("Jessore","Khulna",23.1688,89.2078,2880000),
    ("Narail","Khulna",23.1724,89.5122,764000),
    ("Magura","Khulna",23.4876,89.4199,989000),
    ("Jhenaidah","Khulna",23.5448,89.1541,1935000),
    ("Kushtia","Khulna",23.9013,88.9982,2017000),
    ("Chuadanga","Khulna",23.6401,88.8420,1302000),
    ("Meherpur","Khulna",23.7622,88.6318,812000),
    ("Barisal","Barisal",22.7010,90.3535,2343000),
    ("Bhola","Barisal",22.6863,90.6499,1997000),
    ("Patuakhali","Barisal",22.3596,90.3254,1845000),
    ("Pirojpur","Barisal",22.5791,89.9759,1179000),
    ("Jhalokathi","Barisal",22.6430,90.2016,774000),
    ("Barguna","Barisal",22.1500,89.8674,979000),
    ("Rangpur","Rangpur",25.7439,89.2752,2887000),
    ("Gaibandha","Rangpur",25.3285,89.5287,2826000),
    ("Nilphamari","Rangpur",25.9318,88.8560,1931000),
    ("Lalmonirhat","Rangpur",25.9217,89.4500,1323000),
    ("Kurigram","Rangpur",25.8054,89.6358,2232000),
    ("Thakurgaon","Rangpur",26.0317,88.4281,1521000),
    ("Dinajpur","Rangpur",25.6217,88.6354,3193000),
    ("Panchagarh","Rangpur",26.3410,88.5561,1033000),
]

FACILITY_TYPES = [
    ("district_hospital", 0.05, 300, 600, 20, 50),    # (type, weight, min_beds, max_beds, min_icu, max_icu)
    ("upazila_health_complex", 0.35, 30, 80, 2, 8),
    ("union_health_centre", 0.30, 5, 20, 0, 0),
    ("private_clinic", 0.20, 10, 100, 2, 15),
    ("community_clinic", 0.10, 0, 5, 0, 0),
]


def generate_facilities(seed: int, target: int = 650) -> list:
    rng = random.Random(seed)
    facilities = []
    fac_id = 1

    total_pop = sum(d[4] for d in DISTRICTS)

    for dist_name, division, lat, lon, pop in DISTRICTS:
        # Allocate facilities proportionally to population, minimum 6 per district
        share = max(6, round(target * pop / total_pop))

        for _ in range(share):
            ftype_row = rng.choices(FACILITY_TYPES, weights=[r[1] for r in FACILITY_TYPES])[0]
            ftype, _, min_b, max_b, min_icu, max_icu = ftype_row

            beds = rng.randint(min_b, max_b) if max_b > 0 else 0
            icu_beds = rng.randint(min_icu, max_icu) if max_icu > 0 else 0
            icu_beds = min(icu_beds, beds)  # ICU can't exceed total beds

            # Jitter lat/lon within ~25km of district centroid
            jitter_lat = lat + rng.uniform(-0.22, 0.22)
            jitter_lon = lon + rng.uniform(-0.22, 0.22)

            facilities.append({
                "facility_id":   f"FAC{fac_id:05d}",
                "facility_name": f"{dist_name} {ftype.replace('_',' ').title()} {fac_id}",
                "facility_type": ftype,
                "district":      dist_name,
                "division":      division,
                "latitude":      round(jitter_lat, 5),
                "longitude":     round(jitter_lon, 5),
                "total_beds":    beds,
                "icu_beds":      icu_beds,
                "active":        1 if rng.random() > 0.02 else 0,  # ~2% inactive
            })
            fac_id += 1

    return facilities


def main():
    parser = argparse.ArgumentParser(description="Generate S5 facility registry")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--target", type=int, default=650, help="Approx total facilities")
    args = parser.parse_args()

    print(f"Generating facility registry (seed={args.seed}, target~{args.target})...")
    facilities = generate_facilities(seed=args.seed, target=args.target)

    out_file = OUTPUT_DIR / "facility_registry.csv"
    fieldnames = ["facility_id","facility_name","facility_type","district","division",
                  "latitude","longitude","total_beds","icu_beds","active"]

    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(facilities)

    print(f"[OK] {len(facilities)} facilities -> {out_file}")
    by_type = {}
    for fac in facilities:
        by_type[fac["facility_type"]] = by_type.get(fac["facility_type"], 0) + 1
    for t, c in sorted(by_type.items()):
        print(f"     {t:30s}: {c}")


if __name__ == "__main__":
    main()
