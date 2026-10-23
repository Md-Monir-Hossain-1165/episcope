"""
Quality Rules standalone module – used by silver/transform.py
Also exports a summary table to pipeline/gold/quality_report/

Can be run standalone to validate any Parquet file:
    python pipeline/quality/rules.py --src pipeline/bronze/encounters
"""

import argparse, time
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

QUALITY_REPORT_DIR = Path("pipeline/gold/quality_report")
QUALITY_REPORT_DIR.mkdir(parents=True, exist_ok=True)

VALID_ICD10 = {
    "A90","A91","A00","A01","A09","J10","J11",
    "B50","B51","B54","J00","K30","N39","R50","M79","Z00"
}
VALID_OUTCOMES  = ["outpatient", "discharged", "died"]
VALID_SEX       = ["M", "F"]
MAX_DELAY_DAYS  = 21
MAX_AGE         = 150


def run_rules(df: DataFrame, spark: SparkSession, valid_districts: list) -> dict:
    """
    Run all 12 quality rules. Returns a dict with pass_rate per rule.
    Does NOT modify the dataframe — purely reporting.
    """
    n = df.count()
    if n == 0:
        return {}

    results = {"total_rows": n, "rules": {}}

    def check(name, cond):
        fail = df.filter(~cond).count()
        results["rules"][name] = {
            "failed": fail,
            "passed": n - fail,
            "pass_rate_pct": round((n - fail) / n * 100, 2)
        }

    check("R01_encounter_id_not_null",  F.col("encounter_id").isNotNull())
    check("R02_facility_id_not_null",   F.col("facility_id").isNotNull())
    check("R03_age_in_range",           (F.col("age_years") >= 0) & (F.col("age_years") <= MAX_AGE))
    check("R04_sex_valid",              F.col("sex").isin(VALID_SEX))
    check("R05_icd10_valid",            F.substring("icd10_code", 1, 3).isin(list(VALID_ICD10)))
    check("R06_onset_le_report",        F.col("onset_date") <= F.col("report_date"))
    check("R07_district_valid",         F.col("district").isin(valid_districts))
    check("R08_onset_date_not_null",    F.col("onset_date").isNotNull())
    check("R09_report_date_not_null",   F.col("report_date").isNotNull())
    check("R10_delay_le_21d",
          F.datediff(F.to_date("report_date"), F.to_date("onset_date")) <= MAX_DELAY_DAYS)
    check("R11_outcome_valid",          F.col("outcome").isin(VALID_OUTCOMES))
    check("R12_no_future_onset",
          F.to_date("onset_date") <= F.lit("2025-12-31"))

    return results


def save_report(results: dict, source_tag: str):
    import pandas as pd
    rows = []
    for rule, info in results.get("rules", {}).items():
        rows.append({
            "run_at":        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "source":        source_tag,
            "rule":          rule,
            "failed_rows":   info["failed"],
            "passed_rows":   info["passed"],
            "pass_rate_pct": info["pass_rate_pct"],
            "total_rows":    results["total_rows"],
        })
    df = pd.DataFrame(rows)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out = QUALITY_REPORT_DIR / f"qr_{source_tag}_{ts}.parquet"
    df.to_parquet(str(out), index=False)
    print(f"[OK] Quality report saved -> {out}")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True, help="Path to Parquet source")
    parser.add_argument("--tag", default="manual_check")
    args = parser.parse_args()

    spark = (SparkSession.builder
             .appName("EpiScope-QualityCheck")
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.src)
    pop = spark.read.csv(
        "pipeline/bronze/population/bd_districts_population.csv",
        header=True).select("district")
    valid_districts = [r["district"] for r in pop.collect()]

    print(f"\nRunning 12 quality rules on {df.count():,} rows from {args.src}...")
    results = run_rules(df, spark, valid_districts)

    # Print summary
    print(f"\n{'Rule':<30} {'Pass%':>8} {'Failed':>10}")
    print("-" * 52)
    for rule, info in results["rules"].items():
        print(f"{rule:<30} {info['pass_rate_pct']:>7.2f}% {info['failed']:>10,}")

    save_report(results, args.tag)
    spark.stop()


if __name__ == "__main__":
    main()
