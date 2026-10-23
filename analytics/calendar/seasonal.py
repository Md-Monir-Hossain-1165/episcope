"""District disease preparedness calendar derived from historical weekly incidence."""
from pathlib import Path
import pandas as pd
ROOT=Path(__file__).resolve().parents[2]; OUT=ROOT/"analytics/calendar/preparedness_calendar.parquet"
def main():
    d=pd.read_parquet(ROOT/"pipeline/gold/epi_weekly",columns=["district","division","disease_group","epi_week","case_count"]); d["cases"]=d.case_count.mask(d.case_count<0).fillna(0); a=d.groupby(["district","division","disease_group","epi_week"],as_index=False).cases.mean(); a["risk_level"]=a.groupby(["district","disease_group"]).cases.transform(lambda x:pd.qcut(x.rank(method="first"),3,labels=["routine","prepare","peak"])).astype(str); a["recommended_action"]=a.risk_level.map({"routine":"routine surveillance","prepare":"pre-position supplies and intensify surveillance","peak":"activate response readiness and weekly review"}); OUT.parent.mkdir(parents=True,exist_ok=True); a.to_parquet(OUT,index=False); print(f"[OK] {len(a):,} preparedness weeks -> {OUT}")
if __name__=="__main__": main()
