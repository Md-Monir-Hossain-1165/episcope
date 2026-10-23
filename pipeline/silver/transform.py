"""
Silver Transform – ING-2, ING-3, ING-4, DQ-1, DQ-2, DQ-3, PRV-1
Single-pass quality tagging, broadcast join, dynamic partition overwrite.
Run: python pipeline/silver/transform.py [--date 2023_01]
"""
import argparse, hashlib, os, time
from pathlib import Path

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

BASE_DIR     = Path(__file__).resolve().parent.parent.parent
BRONZE_ENC   = BASE_DIR / "pipeline" / "bronze"  / "encounters"
POP_CSV      = BASE_DIR / "pipeline" / "bronze"  / "population" / "bd_districts_population.csv"
SILVER_ENC   = BASE_DIR / "pipeline" / "silver"  / "encounters"
QUARANTINE   = BASE_DIR / "pipeline" / "silver"  / "quarantine"
QUALITY_LOG  = BASE_DIR / "pipeline" / "gold"    / "quality_report"

for p in [SILVER_ENC, QUARANTINE, QUALITY_LOG]:
    p.mkdir(parents=True, exist_ok=True)

SALT = os.getenv("PATIENT_ID_SALT", "episcope_dev_salt_2026")

VALID_ICD10    = ["A90","A91","A00","A01","A09","J10","J11","B50","B51","B54","J00","K30","N39","R50","M79","Z00"]
VALID_OUTCOMES = ["outpatient","discharged","died"]
VALID_SEX      = ["M","F"]


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName("EpiScope-Silver")
            .master("local[*]")
            .config("spark.driver.memory",               "4g")
            .config("spark.sql.shuffle.partitions",      "16")
            .config("spark.hadoop.fs.defaultFS",         "file:///")
            .config("spark.sql.parquet.compression.codec","snappy")
            .getOrCreate())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="Single month e.g. 2023_01")
    args = parser.parse_args()

    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    import pandas as pd

    t0 = time.time()
    print("=== EpiScope Silver Transform (ING-2/3/4, DQ-1/2/3, PRV-1) ===")

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Load population/division for broadcast join
    pop_df = (spark.read.csv(str(POP_CSV), header=True)
                   .select("district", "division"))
    valid_districts = [r["district"] for r in pop_df.collect()]

    # Resolve encounter files
    pattern = f"encounters_{args.date}.parquet" if args.date else "encounters_*.parquet"
    files   = sorted(BRONZE_ENC.glob(pattern))
    if not files:
        print("[WARN] No encounter files found."); spark.stop(); return

    print(f"Loading {len(files)} file(s)...")
    df = spark.read.parquet(*[str(f) for f in files])
    n_in = df.count()
    print(f"  Rows in bronze : {n_in:,}")

    # PRV-1: salt-hash patient_id, drop original.  Use Spark SQL rather than a
    # Python UDF: this avoids per-row Python worker overhead and works on Windows
    # installations where a Spark worker may not resolve the same interpreter.
    df = (df.withColumn(
            "patient_id_hash",
            F.sha2(F.concat(F.lit(SALT), F.coalesce(F.col("patient_id"), F.lit(""))), 256),
        )
        .drop("patient_id"))

    # DQ-1: Single-pass quality tag (most-specific rule wins)
    df = df.withColumn("_qreason",
        F.when(F.col("encounter_id").isNull(),                                   "R01:null_encounter_id")
        .when(F.col("facility_id").isNull(),                                     "R02:null_facility_id")
        .when(F.col("age_years").isNull() | (F.col("age_years").cast("double") < 0) | (F.col("age_years").cast("double") > 150), "R03:invalid_age")
        .when(~F.col("sex").isin(VALID_SEX),                                     "R04:invalid_sex")
        .when(~F.substring("icd10_code",1,3).isin(VALID_ICD10),                 "R05:invalid_icd10")
        .when(F.col("onset_date").isNull(),                                      "R08:null_onset_date")
        .when(F.col("report_date").isNull(),                                     "R09:null_report_date")
        .when(F.col("onset_date") > F.col("report_date"),                       "R06:onset_gt_report")
        .when(F.datediff(F.to_date("report_date"), F.to_date("onset_date")) > 21,"R10:delay_gt_21d")
        .when(~F.col("district").isin(valid_districts),                         "R07:invalid_district")
        .when(~F.col("outcome").isin(VALID_OUTCOMES),                           "R12:invalid_outcome")
        .otherwise(F.lit(None).cast(StringType())))

    df.cache(); df.count()  # materialise

    clean = df.filter(F.col("_qreason").isNull()).drop("_qreason")
    quar  = df.filter(F.col("_qreason").isNotNull())

    # R11: de-duplicate encounter_id (keep first)
    clean = clean.dropDuplicates(["encounter_id"])

    n_clean = clean.count()
    n_quar  = n_in - n_clean
    print(f"  Clean          : {n_clean:,}")
    print(f"  Quarantined    : {n_quar:,}")

    # Save quarantine (DQ-1)
    ts   = time.strftime("%Y%m%d_%H%M%S")
    quar.write.mode("overwrite").parquet(str(QUARANTINE / f"quar_{ts}"))
    print(f"  Quarantine  -> quarantine/quar_{ts}")
    df.unpersist()

    # ING-3: add epi_year, epi_week, age_band; broadcast-join division (ENG-3 opt)
    clean = (clean
        .withColumn("epi_year",  F.year(F.to_date("onset_date")))
        .withColumn("epi_week",  F.weekofyear(F.to_date("onset_date")))
        .withColumn("age_band",
            F.when(F.col("age_years") < 5,  "0-4")
             .when(F.col("age_years") < 15, "5-14")
             .when(F.col("age_years") < 30, "15-29")
             .when(F.col("age_years") < 45, "30-44")
             .when(F.col("age_years") < 60, "45-59")
             .otherwise("60+"))
        .join(F.broadcast(pop_df), on="district", how="left"))  # ENG-3: broadcast join

    # ING-4: dynamic overwrite = idempotent per partition
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    print(f"  Writing silver -> {SILVER_ENC}")
    (clean.write.mode("overwrite")
          .partitionBy("division","epi_year","epi_week")
          .parquet(str(SILVER_ENC)))
    print(f"  [OK] Silver written")

    # DQ-2: quality report
    rule_counts = {r["_qreason"]: r["count"]
                   for r in quar.groupBy("_qreason").count().collect()}
    qr = pd.DataFrame([{
        "run_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "S4_encounters", "batch": args.date or "all",
        "rows_in": n_in, "rows_out": n_clean, "rows_quarantined": n_quar,
        "elapsed_sec": round(time.time()-t0, 1), **rule_counts
    }])
    qr.to_parquet(str(QUALITY_LOG / f"qr_{ts}.parquet"), index=False)
    print(f"  [OK] Quality report -> quality_report/qr_{ts}.parquet")

    spark.stop()
    print(f"[DONE] Silver transform in {round(time.time()-t0,1)}s")


if __name__ == "__main__":
    main()
