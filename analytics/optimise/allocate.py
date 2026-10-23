"""PuLP equitable allocation of national emergency supplies (AN-7)."""
from pathlib import Path
import pandas as pd
import pulp
ROOT=Path(__file__).resolve().parents[2]; OUT=ROOT/"analytics/optimise/allocation.parquet"
def main():
    epi=pd.read_parquet(ROOT/"pipeline/gold/epi_weekly",columns=["district","disease_group","epi_year","epi_week","case_count"])
    latest=epi.sort_values(["epi_year","epi_week"]).groupby(["district","disease_group"],as_index=False).tail(1); latest["need"]=latest.case_count.clip(lower=0).fillna(0)*2
    need=latest.groupby("district",as_index=False).need.sum(); budget=max(1000,int(need.need.sum()*0.6)); p=pulp.LpProblem("EpiScopeAllocation",pulp.LpMaximize); x={d:pulp.LpVariable(f"x_{i}",lowBound=0,cat="Integer") for i,d in enumerate(need.district)}; u={d:pulp.LpVariable(f"u_{i}",lowBound=0) for i,d in enumerate(need.district)}
    p += pulp.lpSum(u.values()); p += pulp.lpSum(x.values()) <= budget
    for r in need.itertuples(): p += u[r.district] <= r.need; p += u[r.district] <= x[r.district]; p += x[r.district] >= min(10, int(r.need*0.1))
    p.solve(pulp.PULP_CBC_CMD(msg=False)); need["allocated_units"]=need.district.map(lambda d:int(pulp.value(x[d]) or 0)); need["unmet_need"]=need.need-need.allocated_units; need["national_budget"]=budget; OUT.parent.mkdir(parents=True,exist_ok=True); need.to_parquet(OUT,index=False); print(f"[OK] allocated {need.allocated_units.sum():,}/{budget:,} units -> {OUT}")
if __name__=="__main__": main()
