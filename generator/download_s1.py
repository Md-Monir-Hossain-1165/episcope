"""
S1 Downloader – Bangladesh District Boundaries (ADM2 GeoJSON)
Source: geoBoundaries / HDX
Saves to: pipeline/bronze/boundaries/
"""

import requests
import json
from pathlib import Path

OUTPUT_DIR = Path("pipeline/bronze/boundaries")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# geoBoundaries provides clean ADM2 (district-level) GeoJSON for Bangladesh
# BGD = ISO code for Bangladesh, ADM2 = district level
GEOBOUNDARIES_URL = (
    "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/"
    "gbOpen/BGD/ADM2/geoBoundaries-BGD-ADM2.geojson"
)

# Alternative: HDX direct link
HDX_URL = (
    "https://data.humdata.org/dataset/"
    "cod-ab-bgd/resource/1af8d1b3-5578-4f8e-bdd3-6afa69785e4e/download/"
    "bgd_adm_bbs_20201113_SHP.zip"
)

def download_geojson():
    out_file = OUTPUT_DIR / "bangladesh_adm2.geojson"
    if out_file.exists():
        print(f"[SKIP] Already downloaded: {out_file}")
        return out_file

    print(f"Downloading Bangladesh ADM2 boundaries...")
    response = requests.get(GEOBOUNDARIES_URL, timeout=60)
    if response.status_code == 200:
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(response.text)
        data = json.loads(response.text)
        print(f"[OK] Saved {len(data['features'])} districts -> {out_file}")
        return out_file
    else:
        print(f"[ERROR] Status {response.status_code}")
        print("Manual alternative: Download from https://www.geoboundaries.org/country/BGD/")
        print("Choose BGD > ADM2 > GeoJSON and save to pipeline/bronze/boundaries/bangladesh_adm2.geojson")
        return None

def extract_centroids(geojson_path):
    """Extract district centroids and save as CSV for S3 weather fetching."""
    import geopandas as gpd
    import pandas as pd

    gdf = gpd.read_file(geojson_path)
    gdf["centroid"] = gdf.geometry.centroid
    gdf["centroid_lat"] = gdf["centroid"].y
    gdf["centroid_lon"] = gdf["centroid"].x

    # Try common column name variations
    name_col = next((c for c in ["shapeName", "ADM2_EN", "NAME_2", "district"] if c in gdf.columns), None)
    if name_col:
        gdf = gdf.rename(columns={name_col: "district"})

    out_csv = OUTPUT_DIR / "district_centroids.csv"
    gdf[["district", "centroid_lat", "centroid_lon"]].to_csv(out_csv, index=False)
    print(f"[OK] Saved {len(gdf)} district centroids -> {out_csv}")
    return out_csv

if __name__ == "__main__":
    geojson = download_geojson()
    if geojson:
        try:
            extract_centroids(geojson)
        except ImportError:
            print("[INFO] geopandas not installed – centroids will use the S2 CSV instead.")
        print("\nS1 complete. GeoJSON ready for dashboard choropleth (VIS-2).")
