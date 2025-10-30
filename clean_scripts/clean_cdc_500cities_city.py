"""
Clean CDC 500 Cities city-level (2016–2019) if present.
Expected input filenames (any that exist will be processed):
  500_Cities__City-level_Data_(GIS_Friendly_Format),_{YEAR}_release_*.csv

Output:
  cleaned/500cities_city_{YEAR}.csv – trimmed city-level indicators
Notes:
 - Keeps essential columns and renames to a consistent schema.
 - Does NOT pivot by measure; leaves long form (one row per place+measure).
"""
import glob
import pandas as pd
from pathlib import Path

YEARS = [2016, 2017, 2018, 2019]
OUT_DIR = Path("cleaned")
OUT_DIR.mkdir(exist_ok=True)

# Common columns in 500 Cities City-level (GIS-friendly) exports (vary slightly by year)
CANDIDATES = [
    "PlaceFIPS", "PlaceName", "StateAbbr", "StateDesc", "CityName",
    "MeasureId", "Measure", "Category", "Data_Value", "Low_Confidence_Limit",
    "High_Confidence_Limit", "PopulationCount", "TotalPopulation",
    "Short_Question_Text", "LocationName", "TractFIPS"
]

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def find_files(year):
    patt = f"500_Cities__City-level_Data_(GIS_Friendly_Format),_{year}_release*.csv"
    return glob.glob(patt)

def clean_file(path, year):
    df = read_csv_flexible(path)
    keep = [c for c in CANDIDATES if c in df.columns] or df.columns.tolist()
    out = df[keep].copy()
    out["year"] = year
    if "PlaceFIPS" in out.columns:
        out.rename(columns={"PlaceFIPS":"place_fips"}, inplace=True)
    if "Data_Value" in out.columns:
        out.rename(columns={"Data_Value":"value"}, inplace=True)
    return out

def main():
    summaries = []
    for y in YEARS:
        files = find_files(y)
        if not files:
            print(f"[500 Cities] No files for {y}. Skipping.")
            continue
        frames = [clean_file(f, y) for f in files]
        out = pd.concat(frames, ignore_index=True)
        out_path = OUT_DIR / f"500cities_city_{y}.csv"
        out.to_csv(out_path, index=False)
        summaries.append({"year": y, "rows": len(out), "file": str(out_path)})
        print(f"[500 Cities] Wrote {out_path.name} ({len(out)} rows)")
    pd.DataFrame(summaries).to_csv(OUT_DIR / "500cities_city_summary.csv", index=False)
    print("[500 Cities] Done.")

if __name__ == "__main__":
    main()
PY
