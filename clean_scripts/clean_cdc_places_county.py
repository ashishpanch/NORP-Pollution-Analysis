"""
Clean CDC PLACES county-level (2020–2025) if present.
Expected input filenames (any that exist will be processed):
  PLACES__County_Data_(GIS_Friendly_Format),_{YEAR}_release*.csv

Output:
  cleaned/places_county_{YEAR}.csv – trimmed county-level indicators (long form)
"""
import glob
import pandas as pd
from pathlib import Path

YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
OUT_DIR = Path("cleaned")
OUT_DIR.mkdir(exist_ok=True)

CANDIDATES = [
    "CountyFIPS","CountyName","StateAbbr","StateDesc",
    "MeasureId","Measure","Category","Data_Value","Low_Confidence_Limit","High_Confidence_Limit",
    "PopulationCount","TotalPopulation","Short_Question_Text"
]

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def find_files(year):
    patt = f"PLACES__County_Data_(GIS_Friendly_Format),_{year}_release*.csv"
    return glob.glob(patt)

def clean_file(path, year):
    df = read_csv_flexible(path)
    keep = [c for c in CANDIDATES if c in df.columns] or df.columns.tolist()
    out = df[keep].copy()
    out["year"] = year
    if "CountyFIPS" in out.columns:
        out.rename(columns={"CountyFIPS":"county_fips"}, inplace=True)
    if "Data_Value" in out.columns:
        out.rename(columns={"Data_Value":"value"}, inplace=True)
    return out

def main():
    summaries = []
    for y in YEARS:
        files = find_files(y)
        if not files:
            print(f"[PLACES] No files for {y}. Skipping.")
            continue
        frames = [clean_file(f, y) for f in files]
        out = pd.concat(frames, ignore_index=True)
        out_path = OUT_DIR / f"places_county_{y}.csv"
        out.to_csv(out_path, index=False)
        summaries.append({"year": y, "rows": len(out), "file": str(out_path)})
        print(f"[PLACES] Wrote {out_path.name} ({len(out)} rows)")
    pd.DataFrame(summaries).to_csv(OUT_DIR / "places_county_summary.csv", index=False)
    print("[PLACES] Done.")

if __name__ == "__main__":
    main()
PY
