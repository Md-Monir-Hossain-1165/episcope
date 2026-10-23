"""
Incremental Load – ING-4
Drop new files into pipeline/bronze/encounters/incremental/ and run this.
Idempotent: dynamic partition overwrite, never creates duplicates.
Run: python pipeline/silver/incremental.py
"""
import hashlib, os, time
from pathlib import Path

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

BASE_DIR     = Path(__file__).resolve().parent.parent.parent
INCREMENTAL  = BASE_DIR / "pipeline" / "bronze" / "encounters" / "incremental"
POP_CSV      = BASE_DIR / "pipeline" / "bronze" / "population" / "bd_districts_population.csv"
SILVER_ENC   = BASE_DIR / "pipeline" / "silver" / "encounters"
INCREMENTAL.mkdir(parents=True, exist_ok=True)

SALT = os.getenv("PATIENT_ID_SALT", "episcope_dev_salt_2026")
VALID_ICD10    = ["A90","A91","A00","A01","A09","J10","J11","B50","B51","B54","J00","K30","N39","R50","M79","Z00"]
VALID_OUTCOMES = ["outpatient","discharged","died"]
VALID_SEX      = ["M","F"]


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName("EpiScope-Incremental")
            .master("local[*]")
            .config("spark.driver.memory",          "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.hadoop.fs.defaultFS",    "file:///")
            .getOrCreate())


def main():
    files = list(INCREMENTAL.glob("*.parquet")) + list(INCREMENTAL.glob("*.csv"))
    if not files:
        print(f"[INFO] No incremental files in {INCREMENTAL}")
        print("       Drop encounter Parquet/CSV files there and re-run.")
        return

    print(f"=== EpiScope Incremental Load (ING-4) — {len(files)} file(s) ===")
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType
    from functools import reduce

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    pop_df = spark.read.csv(str(POP_CSV), header=True).select("district","division")
    valid_districts = [r["district"] for r in pop_df.collect()]

    # Read all incremental files
    parts = [spark.read.parquet(str(f)) if f.suffix == ".parquet"
             else spark.read.csv(str(f), header=True, inferSchema=True)
             for f in files]
    df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), parts)

    # Hash patient_id
    if "patient_id" in df.columns:
        df = (df.withColumn(
                "patient_id_hash",
                F.sha2(F.concat(F.lit(SALT), F.coalesce(F.col("patient_id"), F.lit(""))), 256),
            )
            .drop("patient_id"))

    # Quality tag (same rules as silver)
    df = df.withColumn("_qreason",
        F.when(F.col("encounter_id").isNull(), "R01:null_encounter_id")
        .when(F.col("facility_id").isNull(),   "R02:null_facility_id")
        .when(~F.col("sex").isin(VALID_SEX),   "R04:invalid_sex")
        .when(~F.substring("icd10_code",1,3).isin(VALID_ICD10), "R05:invalid_icd10")
        .when(F.col("onset_date").isNull(),    "R08:null_onset_date")
        .when(F.col("report_date").isNull(),   "R09:null_report_date")
        .when(~F.col("district").isin(valid_districts), "R07:invalid_district")
        .otherwise(F.lit(None).cast(StringType())))

    clean = (df.filter(F.col("_qreason").isNull())
               .drop("_qreason")
               .dropDuplicates(["encounter_id"]))

    clean = (clean
        .withColumn("epi_year", F.year(F.to_date("onset_date")))
        .withColumn("epi_week", F.weekofyear(F.to_date("onset_date")))
        .withColumn("age_band",
            F.when(F.col("age_years") < 5, "0-4").when(F.col("age_years") < 15,"5-14")
             .when(F.col("age_years") < 30,"15-29").when(F.col("age_years") < 45,"30-44")
             .when(F.col("age_years") < 60,"45-59").otherwise("60+"))
        .withColumn("_batch_id",  F.lit(f"incremental_{time.strftime('%Y%m%d_%H%M%S')}"))
        .withColumn("_loaded_at", F.lit(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())))
        .withColumn("_source",    F.lit("incremental_drop"))
        .join(F.broadcast(pop_df), on="district", how="left"))

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    clean.write.mode("overwrite").partitionBy("division","epi_year","epi_week").parquet(str(SILVER_ENC))

    n = clean.count()
    print(f"[OK] {n:,} records written to silver (idempotent)")
    print("[INFO] Re-run pipeline/gold/mart.py to refresh gold layer.")
    spark.stop()


if __name__ == "__main__":
    main()
