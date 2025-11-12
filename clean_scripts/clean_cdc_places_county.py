import glob
import pandas as pd
from pathlib import Path

YEARS = [2020, 2021, 2022, 2023, 2024]
OUT_DIR = Path("../cleaned_health_outcomes_data")
OUT_DIR.mkdir(exist_ok=True)

# Always try to keep these ID/demographic fields (18+ may be absent)
BASE_KEEP = ["CountyFIPS", "CountyName", "StateAbbr", "TotalPopulation", "TotalPop18plus"]

ID_RENAME = {
    "CountyFIPS": "county_fips",
    "CountyName": "county_name",
    "StateAbbr": "state",
    "TotalPopulation": "pop_total",
    "TotalPop18plus": "pop_18plus",
}

# Preferred columns (first) with crude fallbacks (later) per metric
MEASURE_ALIASES = {
    "asthma_prev":     ["CASTHMA_AdjPrev", "ASTHMA_AdjPrev", "CASTHMA_CrudePrev", "ASTHMA_CrudePrev"],
    "copd_prev":       ["COPD_AdjPrev", "COPD_CrudePrev"],
    "chd_prev":        ["CHD_AdjPrev", "CHD_CrudePrev"],
    "stroke_prev":     ["STROKE_AdjPrev", "STROKE_CrudePrev"],
    "smoking_prev":    ["CSMOKING_AdjPrev", "SMOKING_AdjPrev", "CSMOKING_CrudePrev", "SMOKING_CrudePrev"],
    "diabetes_prev":   ["DIABETES_AdjPrev", "DIABETES_CrudePrev"],
    "inactivity_prev": ["LPA_AdjPrev", "LPA_CrudePrev"],
    # Optional general health indicators (kept if present)
    "phlth_prev":      ["PHLTH_AdjPrev", "PHLTH_CrudePrev"],
    "mhlth_prev":      ["MHLTH_AdjPrev", "MHLTH_CrudePrev"],
}

OUTPUT_COL_ORDER = [
    "county_fips", "county_name", "state",
    "pop_total", "pop_18plus",
    "asthma_prev", "copd_prev", "chd_prev", "stroke_prev",
    "smoking_prev", "diabetes_prev", "inactivity_prev",
    "phlth_prev", "mhlth_prev",  # optional; included only if present
    "year",
]

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def find_files(year):
    pattern = f"../health_outcomes/PLACES__County_Data_(GIS_Friendly_Format),_{year}_release*.csv"
    return glob.glob(pattern)

def resolve_metric(df, aliases, metric_name, year, path):
    for col in aliases:
        if col in df.columns:
            print(f"[PLACES][{year}] {metric_name} <= {col} ({Path(path).name})")
            return pd.to_numeric(df[col], errors="coerce")
    print(f"[PLACES][{year}] WARNING: {metric_name} missing in {Path(path).name} (tried {aliases})")
    return pd.Series([pd.NA] * len(df), dtype="float")

def clean_file(path, year):
    raw = read_csv_flexible(path)

    # Start with IDs/demographics that exist
    id_cols_present = [c for c in BASE_KEEP if c in raw.columns]
    out = raw[id_cols_present].copy()
    out.rename(columns={k:v for k,v in ID_RENAME.items() if k in out.columns}, inplace=True)

    # FIPS formatting
    if "county_fips" in out.columns:
        out["county_fips"] = out["county_fips"].astype(str).str.zfill(5)

    # Numeric demographics
    for col in ("pop_total", "pop_18plus"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

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
            print(f"[PLACES] No files for {year}. Skipping.")
            continue

        frames = [clean_file(f, year) for f in files]
        combined = pd.concat(frames, ignore_index=True)

        out_path = OUT_DIR / f"places_county_{year}.csv"
        combined.to_csv(out_path, index=False)
        print(f"[PLACES] Wrote {out_path.name} ({len(combined)} rows)")
        summaries.append({"year": year, "rows": len(combined), "file": str(out_path)})

    pd.DataFrame(summaries).to_csv(OUT_DIR / "places_county_summary.csv", index=False)
    print("[PLACES] Done.")

if __name__ == "__main__":
    main()
