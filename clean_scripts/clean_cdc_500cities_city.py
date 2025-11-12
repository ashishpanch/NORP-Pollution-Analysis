import glob
import pandas as pd
from pathlib import Path

YEARS = [2016, 2017, 2018, 2019]
OUT_DIR = Path("../cleaned_health_outcomes_data")
OUT_DIR.mkdir(exist_ok=True)

# Accept a few possible population columns; map all to pop_2010
BASE_KEEP = ["PlaceFIPS", "PlaceName", "StateAbbr", "Population2010", "PopulationCount", "TotalPopulation"]

ID_RENAME = {
    "PlaceFIPS": "place_fips",
    "PlaceName": "place_name",
    "StateAbbr": "state",
    "Population2010": "pop_2010",
    "PopulationCount": "pop_2010",
    "TotalPopulation": "pop_2010",
}

MEASURE_ALIASES = {
    "asthma_prev":     ["CASTHMA_AdjPrev", "ASTHMA_AdjPrev", "CASTHMA_CrudePrev", "ASTHMA_CrudePrev"],
    "copd_prev":       ["COPD_AdjPrev", "COPD_CrudePrev"],
    "chd_prev":        ["CHD_AdjPrev", "CHD_CrudePrev"],
    "stroke_prev":     ["STROKE_AdjPrev", "STROKE_CrudePrev"],
    "smoking_prev":    ["CSMOKING_AdjPrev", "SMOKING_AdjPrev", "CSMOKING_CrudePrev", "SMOKING_CrudePrev"],
    "diabetes_prev":   ["DIABETES_AdjPrev", "DIABETES_CrudePrev"],
    "inactivity_prev": ["LPA_AdjPrev", "LPA_CrudePrev"],
    # Optional
    "phlth_prev":      ["PHLTH_AdjPrev", "PHLTH_CrudePrev"],
    "mhlth_prev":      ["MHLTH_AdjPrev", "MHLTH_CrudePrev"],
}

OUTPUT_COL_ORDER = [
    "place_fips", "place_name", "state", "pop_2010",
    "asthma_prev", "copd_prev", "chd_prev", "stroke_prev",
    "smoking_prev", "diabetes_prev", "inactivity_prev",
    "phlth_prev", "mhlth_prev",
    "year",
]

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def clean_numeric_series(s):
    # Handles "123,456" and whitespace before conversion
    return pd.to_numeric(
        s.astype(str).str.replace(r"[,\s]", "", regex=True).replace({"": None}),
        errors="coerce"
    )

def find_files(year):
    pattern = f"../health_outcomes/500_Cities__City-level_Data_(GIS_Friendly_Format),_{year}_release*.csv"
    return glob.glob(pattern)

def resolve_metric(df, aliases, metric_name, year, path):
    for col in aliases:
        if col in df.columns:
            print(f"[500Cities][{year}] {metric_name} <= {col} ({Path(path).name})")
            return pd.to_numeric(df[col], errors="coerce")
    print(f"[500Cities][{year}] WARNING: {metric_name} missing in {Path(path).name} (tried {aliases})")
    return pd.Series([pd.NA] * len(df), dtype="float")

def clean_file(path, year):
    raw = read_csv_flexible(path)

    # IDs/demographics (with population aliases)
    id_cols_present = [c for c in BASE_KEEP if c in raw.columns]
    out = raw[id_cols_present].copy()
    out.rename(columns={k:v for k,v in ID_RENAME.items() if k in out.columns}, inplace=True)

    # Normalize IDs and population
    if "place_fips" in out.columns:
        out["place_fips"] = out["place_fips"].astype(str).str.zfill(7)
    if "pop_2010" in out.columns:
        out["pop_2010"] = clean_numeric_series(out["pop_2010"])

    # Resolve each metric via alias list
    for clean_name, aliases in MEASURE_ALIASES.items():
        out[clean_name] = resolve_metric(raw, aliases, clean_name, year, path)

    out["year"] = year

    # Final column order (only keep columns we actually have)
    ordered = [c for c in OUTPUT_COL_ORDER if c in out.columns]
    return out[ordered]

def main():
    summaries = []
    for year in YEARS:
        files = find_files(year)
        if not files:
            print(f"[500Cities] No files for {year}. Skipping.")
            continue

        frames = [clean_file(f, year) for f in files]
        combined = pd.concat(frames, ignore_index=True)

        out_path = OUT_DIR / f"500cities_city_{year}.csv"
        combined.to_csv(out_path, index=False)
        print(f"[500Cities] Wrote {out_path.name} ({len(combined)} rows)")
        summaries.append({"year": year, "rows": len(combined), "file": str(out_path)})

    pd.DataFrame(summaries).to_csv(OUT_DIR / "500cities_city_summary.csv", index=False)
    print("[500Cities] Done.")

if __name__ == "__main__":
    main()
