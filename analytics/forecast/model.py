"""Per district/disease 1-4 week SARIMA forecasts (AN-4)."""
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

ROOT=Path(__file__).resolve().parents[2]; OUT=ROOT/"analytics/forecast/forecasts.parquet"
def main():
    df=pd.read_parquet(ROOT/"pipeline/gold/epi_weekly", columns=["district","division","disease_group","epi_year","epi_week","case_count"])
    df["date"]=pd.to_datetime(df.epi_year.astype(str)+df.epi_week.astype(str).str.zfill(2)+"-1",format="%G%V-%u")
    df["cases"]=df.case_count.mask(df.case_count<0).fillna(0.0)
    out=[]
    warnings.filterwarnings("ignore")
    for (district,division,disease),g in df.sort_values("date").groupby(["district","division","disease_group"]):
        # Gold can contain more than one row for a district/disease/week when
        # operational batches overlap; construct one weekly surveillance series.
        weekly = g.groupby("date", as_index=True).cases.sum().sort_index()
        y=weekly.asfreq("7D",fill_value=0.0).tail(130)
        try:
            # Short seasonal state gives a tractable local SARIMA fit across the
            # full national panel (64 districts × disease groups).
            fit=SARIMAX(y,order=(1,0,0),seasonal_order=(1,0,0,4),trend="c",enforce_stationarity=False).fit(disp=False,maxiter=5)
            pred=fit.get_forecast(4); means=np.maximum(0,pred.predicted_mean); ci=pred.conf_int()
            for horizon,(d,v) in enumerate(means.items(),1): out.append(dict(district=district,division=division,disease_group=disease,forecast_week=d,horizon_weeks=horizon,forecast_cases=round(float(v),2),lower_95=round(max(0,float(ci.loc[d].iloc[0])),2),upper_95=round(max(0,float(ci.loc[d].iloc[1])),2),model="SARIMA(1,0,0)x(1,0,0,4)"))
        except Exception:
            # Deterministic seasonal-naive fallback retains usable dashboard output.
            for h in range(1,5): out.append(dict(district=district,division=division,disease_group=disease,forecast_week=y.index[-1]+pd.Timedelta(weeks=h),horizon_weeks=h,forecast_cases=round(float(y.tail(4).mean()),2),lower_95=0.0,upper_95=round(float(y.tail(12).quantile(.9)),2),model="seasonal_naive_fallback"))
    r=pd.DataFrame(out); OUT.parent.mkdir(parents=True,exist_ok=True); r.to_parquet(OUT,index=False); print(f"[OK] {len(r):,} forecast rows -> {OUT}")
if __name__=="__main__": main()
