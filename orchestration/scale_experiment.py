"""Reproducible Spark scalability benchmark for EpiScope (ENG-1--ENG-4).

Run after producing encounter datasets under data/scales/sf_<factor>/encounters.
The script never mutates Bronze/Silver/Gold production layers.
"""
import argparse, json, platform, time
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession, functions as F

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "orchestration" / "results" / "scalability.parquet"

def spark():
    return (SparkSession.builder.appName("EpiScope-ScaleExperiment").master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.hadoop.fs.defaultFS", "file:///").getOrCreate())

def benchmark(s, path, sf, logical_repeat=1):
    t = time.perf_counter()
    # Projection + predicate pushdown emulate the Gold weekly mart's principal workload.
    df = s.read.parquet(str(path)).select("district", "disease_group", "onset_date", "encounter_id")
    # When a dedicated generated input is unavailable, repeat the baseline scan
    # only for a non-destructive workload-scaling experiment.  Results retain
    # their method so this is never misrepresented as independently simulated data.
    for _ in range(logical_repeat - 1):
        df = df.unionByName(s.read.parquet(str(path)).select("district", "disease_group", "onset_date", "encounter_id"))
    rows = df.count()
    grouped = (df.withColumn("epi_week", F.weekofyear("onset_date"))
        .groupBy("district", "disease_group", "epi_week").count())
    groups = grouped.count()
    elapsed = time.perf_counter() - t
    return {"scale_factor": sf, "input_rows": rows, "output_groups": groups,
            "elapsed_seconds": round(elapsed, 2), "rows_per_second": round(rows / elapsed, 2),
            "shuffle_partitions": 16, "aqe_enabled": True, "input_method": "generated" if logical_repeat == 1 else "logical_repeat", "status": "complete"}

def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--scales", nargs="+", type=float, default=[.25,.5,1]); args = ap.parse_args()
    s = spark(); s.sparkContext.setLogLevel("WARN"); records=[]
    for sf in args.scales:
        path = ROOT / "data" / "scales" / f"sf_{sf:g}" / "encounters"
        # The established project layout keeps the already-generated SF=.25 data
        # in Bronze; larger experimental inputs are isolated under data/scales.
        if sf == .25 and not path.exists():
            path = ROOT / "pipeline" / "bronze" / "encounters"
        repeat = 1
        if not path.exists() and sf in (.5, 1):
            path = ROOT / "pipeline" / "bronze" / "encounters"
            repeat = int(sf / .25)
        if not path.exists():
            records.append({"scale_factor":sf,"status":"missing_input","input_rows":None,"output_groups":None,"elapsed_seconds":None,"rows_per_second":None,"shuffle_partitions":16,"aqe_enabled":True})
            print(f"[SKIP] SF={sf}: expected {path}")
        else:
            r=benchmark(s,path,sf,repeat); records.append(r); print(f"[OK] SF={sf}: {r['input_rows']:,} rows in {r['elapsed_seconds']}s ({r['input_method']})")
    s.stop(); RESULTS.parent.mkdir(parents=True,exist_ok=True); pd.DataFrame(records).to_parquet(RESULTS,index=False)
    (RESULTS.with_suffix('.json')).write_text(json.dumps(records,indent=2)); print(f"[OK] results -> {RESULTS}")
if __name__ == '__main__': main()
