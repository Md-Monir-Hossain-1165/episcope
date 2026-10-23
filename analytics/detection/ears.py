"""Weekly EARS C2 outbreak detection with a CUSUM comparison (AN-1/AN-3)."""
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "pipeline/gold/epi_weekly"
OUT = ROOT / "analytics/detection/alerts.parquet"

def main():
    df = pd.read_parquet(GOLD, columns=["district","division","disease_group","epi_year","epi_week","case_count"])
    # Suppressed counts must not be interpreted as negative cases.
    df["cases"] = df.case_count.mask(df.case_count < 0).astype(float)
    df["date"] = pd.to_datetime(df.epi_year.astype(str) + df.epi_week.astype(str).str.zfill(2) + "-1", format="%G%V-%u")
    rows = []
    for keys, g in df.sort_values("date").groupby(["district", "division", "disease_group"], sort=False):
        g = g.reset_index(drop=True)
        values = g.cases.to_numpy()
        for i in range(len(g)):
            base = values[max(0, i-9):max(0, i-2)]  # 7-week baseline, 2-week guard band
            base = base[np.isfinite(base)]
            if len(base) < 4 or not np.isfinite(values[i]):
                continue
            mean, sd = float(base.mean()), float(base.std(ddof=1)) if len(base) > 1 else 0.0
            threshold = mean + 3 * max(sd, 1.0)
            score = (values[i] - mean) / max(sd, 1.0)
            # One-sided CUSUM over the same baseline.
            hist = values[:i+1]
            cusum = 0.0
            for x in hist[max(0, i-8):]:
                if np.isfinite(x): cusum = max(0.0, cusum + (x - mean - 0.5 * max(sd, 1.0)))
            rows.append(dict(district=keys[0], division=keys[1], disease_group=keys[2],
                epi_year=int(g.epi_year[i]), epi_week=int(g.epi_week[i]), week_start=g.date[i],
                observed_cases=float(values[i]), baseline_mean=round(mean,2), baseline_sd=round(sd,2),
                c2_threshold=round(threshold,2), c2_score=round(score,2), cusum=round(cusum,2),
                alert_level="high" if values[i] > threshold else ("watch" if values[i] > mean + 2*max(sd,1.0) else "none"),
                ears_c2_alert=bool(values[i] > threshold), cusum_alert=bool(cusum > 3*max(sd,1.0))))
    result = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True); result.to_parquet(OUT, index=False)
    print(f"[OK] {len(result):,} scored weeks; {result.ears_c2_alert.sum():,} EARS C2 alerts -> {OUT}")

if __name__ == "__main__": main()
