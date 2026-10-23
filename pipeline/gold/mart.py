"""
Gold Mart – ING-3, PRV-2, DQ-3
Silver -> Gold: weekly district-disease aggregates with small-cell suppression.
Run: python pipeline/gold/mart.py
"""
import time
from pathlib import Path

BASE_DIR    = Path(__file__).resolve().parent.parent.parent
SILVER_ENC  = BASE_DIR / "pipeline" / "silver" / "encounters"
STOCK_DIR   = BASE_DIR / "pipeline" / "bronze" / "stock"
POP_CSV     = BASE_DIR / "pipeline" / "bronze" / "population" / "bd_districts_population.csv"
GOLD_EPI    = BASE_DIR / "pipeline" / "gold"   / "epi_weekly"
GOLD_STOCK  = BASE_DIR / "pipeline" / "gold"   / "stock_weekly"

for p in [GOLD_EPI, GOLD_STOCK]:
    p.mkdir(parents=True, exist_ok=True)


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName("EpiScope-Gold")
            .master("local[*]")
            .config("spark.driver.memory",          "4g")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.hadoop.fs.defaultFS",    "file:///")
            .getOrCreate())


def suppress(df, col):
    """PRV-2: replace counts 1-4 with -1 (suppressed)."""
    from pyspark.sql import functions as F
    return df.withColumn(col,
        F.when((F.col(col) >= 1) & (F.col(col) <= 4), F.lit(-1))
         .otherwise(F.col(col)))


def build_epi_gold(spark):
    from pyspark.sql import functions as F
    print("\n[GOLD] Building epi_weekly mart...")

    silver = spark.read.parquet(str(SILVER_ENC))

    pop = (spark.read.csv(str(POP_CSV), header=True, inferSchema=True)
                .select("district","population_2022"))

    agg = (silver
        .groupBy("epi_year","epi_week","district","division","disease_group")
        .agg(
            F.count("*").alias("case_count"),
            F.sum("lab_confirmed").alias("lab_confirmed_count"),
            F.sum("admitted").alias("admitted_count"),
            F.sum(F.when(F.col("outcome")=="died",1).otherwise(0)).alias("death_count"),
            F.countDistinct("facility_id").alias("facilities_reporting"),
            F.first("_batch_id").alias("_bronze_batch_id"),   # DQ-3 lineage
        ))

    # PRV-2: suppress small cells
    for col in ["case_count","admitted_count","death_count"]:
        agg = suppress(agg, col)

    # Join population for incidence per 100k
    agg = (agg.join(F.broadcast(pop), on="district", how="left")
              .withColumn("incidence_per_100k",
                  F.when(F.col("case_count") > 0,
                         F.round(F.col("case_count")/F.col("population_2022")*100000, 2))
                   .otherwise(F.lit(None)))
              .withColumn("_gold_created_at",
                  F.lit(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))))

    agg.write.mode("overwrite").partitionBy("epi_year").parquet(str(GOLD_EPI))
    n = agg.count()
    print(f"  [OK] {n:,} rows -> {GOLD_EPI}")
    return n


def build_stock_gold(spark):
    from pyspark.sql import functions as F
    print("\n[GOLD] Building stock_weekly mart...")

    stock_files = sorted(STOCK_DIR.glob("stock_*.parquet"))
    if not stock_files:
        print("  [WARN] No stock files found"); return 0

    stock = spark.read.parquet(*[str(f) for f in stock_files])
    stock = (stock
        .withColumn("epi_year", F.year(F.to_date("snapshot_date")))
        .withColumn("epi_week", F.weekofyear(F.to_date("snapshot_date"))))

    agg = (stock
        .groupBy("epi_year","epi_week","district")
        .agg(
            F.avg("occupied_beds").alias("avg_occupied_beds"),
            F.avg("total_beds").alias("avg_total_beds"),
            F.sum("iv_saline_units").alias("total_iv_saline"),
            F.sum("ns1_kits").alias("total_ns1_kits"),
            F.sum("ors_packets").alias("total_ors_packets"),
            F.sum("antimalarials").alias("total_antimalarials"),
            F.countDistinct("facility_id").alias("facilities_reporting"),
        ))

    agg.write.mode("overwrite").partitionBy("epi_year").parquet(str(GOLD_STOCK))
    n = agg.count()
    print(f"  [OK] {n:,} rows -> {GOLD_STOCK}")
    return n


def main():
    print("=== EpiScope Gold Mart (ING-3, PRV-2, DQ-3) ===")
    t0 = time.time()
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    n1 = build_epi_gold(spark)
    n2 = build_stock_gold(spark)
    spark.stop()
    print(f"\n[DONE] Gold complete in {round(time.time()-t0,1)}s — {n1:,} epi rows, {n2:,} stock rows")


if __name__ == "__main__":
    main()
