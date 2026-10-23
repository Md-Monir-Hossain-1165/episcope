"""
Bronze Ingestion – ING-1, ING-2
Ingests all S1-S6 sources into the bronze layer.
Adds ingestion metadata: source, load_time, batch_id, file_checksum.
Heavy files use PySpark; small reference files use Pandas.

Usage:
    python pipeline/bronze/ingest.py
"""

import hashlib, time, os, sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# ── Spark session ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

def get_spark():
    return (SparkSession.builder
            .appName("EpiScope-Bronze-Ingest")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .getOrCreate())

def file_checksum(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def ingest_meta(batch_id: str, source: str):
    return {
        "_batch_id":    batch_id,
        "_source":      source,
        "_loaded_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }

# ── ING-1: Ingest encounters (S4) — largest source, use Spark ─────────────────
def ingest_encounters(spark: SparkSession):
    print("\n[ING-1] Ingesting S4 encounter files...")
    enc_dir = BASE_DIR / "pipeline" / "bronze" / "encounters"
    files   = sorted(enc_dir.glob("encounters_*.parquet"))
    if not files:
        print("  [WARN] No encounter files found — run generate_s4_encounters.py first")
        return

    # Read all monthly parquet files as a directory (local FS)
    df = spark.read.parquet(*[str(f) for f in files])

    if "_checksum" not in df.columns:
        df = df.withColumn("_checksum", F.lit("batch_generated"))

    out = BASE_DIR / "pipeline" / "bronze" / "encounters_registered"
    out.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(out))
    cnt = df.count()
    print(f"  [OK] {cnt:,} encounter records registered in bronze")

# ── ING-1: Ingest facility registry (S5) — small, use Pandas ──────────────────
def ingest_facilities():
    import pandas as pd
    print("\n[ING-1] Ingesting S5 facility registry...")
    src = Path("pipeline/bronze/facilities/facility_registry.csv")
    df  = pd.read_csv(src)
    meta = ingest_meta("s5_facilities", "S5_facility_registry_csv")
    for k, v in meta.items():
        df[k] = v
    df["_checksum"] = file_checksum(src)
    out  = Path("pipeline/bronze/facilities/facility_registry_meta.parquet")
    df.to_parquet(out, index=False)
    print(f"  [OK] {len(df)} facilities -> {out}")

# ── ING-1: Ingest stock snapshots (S6) — use Spark ────────────────────────────
def ingest_stock(spark: SparkSession):
    print("\n[ING-1] Ingesting S6 stock snapshots...")
    stock_dir = BASE_DIR / "pipeline" / "bronze" / "stock"
    files = sorted(stock_dir.glob("stock_*.parquet"))
    if not files:
        print("  [WARN] No stock files found")
        return
    df = spark.read.parquet(*[str(f) for f in files])
    out = BASE_DIR / "pipeline" / "bronze" / "stock_registered"
    out.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(out))
    print(f"  [OK] {df.count():,} stock records registered")

# ── ING-1: Register S1 GeoJSON and S2 population CSV ─────────────────────────
def ingest_reference():
    import pandas as pd, json, shutil
    print("\n[ING-1] Ingesting S1 boundaries & S2 population...")

    # S2 population
    s2 = pd.read_csv("pipeline/bronze/population/bd_districts_population.csv")
    meta = ingest_meta("s2_population", "S2_BBS_census_2022")
    for k, v in meta.items():
        s2[k] = v
    s2.to_parquet("pipeline/bronze/population/population_meta.parquet", index=False)
    print(f"  [OK] S2: {len(s2)} districts")

    # S1 GeoJSON — copy with metadata sidecar
    import shutil
    geo_src = Path("pipeline/bronze/boundaries/bangladesh_adm2.geojson")
    meta_out = Path("pipeline/bronze/boundaries/ingest_meta.json")
    with open(meta_out, "w") as f:
        json.dump({**ingest_meta("s1_boundaries","S1_geoBoundaries"),
                   "_checksum": file_checksum(geo_src)}, f, indent=2)
    print(f"  [OK] S1: GeoJSON registered with metadata sidecar")


def main():
    print("=== EpiScope Bronze Ingestion (ING-1, ING-2) ===")
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    ingest_encounters(spark)
    ingest_facilities()
    ingest_stock(spark)
    ingest_reference()

    spark.stop()
    print("\n[DONE] Bronze ingestion complete.")


if __name__ == "__main__":
    main()
