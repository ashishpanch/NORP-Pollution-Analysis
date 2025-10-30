"""
Clean EPA annual concentration-by-monitor files (2016–2025).
Expected input filenames in the working directory:
  annual_conc_by_monitor_{YEAR}.csv  (e.g., annual_conc_by_monitor_2025.csv)

Output:
  cleaned/epa_conc_by_county_{YEAR}.csv – county-year pollutant stats
  cleaned/epa_conc_by_monitor_{YEAR}.csv – trimmed monitor-level file
"""
import os
import pandas as pd
from pathlib import Path

YEARS = list(range(2016, 2026))
IN_PAT = "annual_conc_by_monitor_{year}.csv"
OUT_DIR = Path("cleaned")
OUT_DIR.mkdir(exist_ok=True)

# Columns we try to keep if present (EPA AirData typical schema)
KEEP_MONITOR_COLS = [
    "State Code","County Code","Site Num","POC","Latitude","Longitude","Datum",
    "Parameter Name","Pollutant Name","CBSA Code","CBSA Name",
    "Metric Used","Method Name","Sample Duration","Units of Measure",
    "Observation Count","Observation Percent","Arithmetic Mean",
    "1st Max Value","1st Max Date","AQI","Date of Last Change","Year"
]

# For aggregation by county-year-pollutant
AGG_MAP = {
    "Arithmetic Mean": "mean",
    "Observation Count": "sum",
    "AQI": "max",
}

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def normalize_fips(row):
    s = str(row.get("State Code","")).split(".")[0].zfill(2)
    c = str(row.get("County Code","")).split(".")[0].zfill(3)
    return s + c

def clean_year(year: int):
    in_name = IN_PAT.format(year=year)
    if not os.path.exists(in_name):
        print(f"[EPA CONC] Missing {in_name} – skipping {year}")
        return None, None
    df = read_csv_flexible(in_name)

    # Ensure a Year column exists
    if "Year" not in df.columns:
        df["Year"] = year

    # Trim to known columns if present
    keep = [c for c in KEEP_MONITOR_COLS if c in df.columns]
    df_trim = df[keep].copy() if keep else df.copy()

    # Standardize a few names if alternate headers exist
    if "Parameter Name" not in df_trim.columns and "Pollutant Name" in df_trim.columns:
        df_trim["Parameter Name"] = df_trim["Pollutant Name"]

    # Build county FIPS
    if "State Code" in df_trim.columns and "County Code" in df_trim.columns:
        df_trim["county_fips"] = df_trim.apply(normalize_fips, axis=1)
    else:
        df_trim["county_fips"] = None

    # Save trimmed monitor-level file
    out_mon = OUT_DIR / f"epa_conc_by_monitor_{year}.csv"
    df_trim.to_csv(out_mon, index=False)

    # ---- Aggregate to county-year-pollutant ----
    group_keys = []
    if "county_fips" in df_trim.columns:
        group_keys.append("county_fips")
    if "Parameter Name" in df_trim.columns:
        group_keys.append("Parameter Name")
    if "Year" in df_trim.columns:
        group_keys.append("Year")

    if not group_keys:
        print(f"[EPA CONC] No grouping keys for {year}; skipping county agg.")
        return out_mon, None

    # Use only columns available for aggregation
    aggable = {k:v for k,v in AGG_MAP.items() if k in df_trim.columns}
    if not aggable:
        print(f"[EPA CONC] No numeric columns to aggregate for {year}; skipping county agg.")
        return out_mon, None

    df_county = df_trim.groupby(group_keys, dropna=False).agg(aggable).reset_index()
    df_county.rename(columns={
        "Arithmetic Mean": "annual_mean",
        "Observation Count": "n_obs",
        "AQI": "max_aqi"
    }, inplace=True)

    out_cty = OUT_DIR / f"epa_conc_by_county_{year}.csv"
    df_county.to_csv(out_cty, index=False)
    print(f"[EPA CONC] Wrote {out_mon.name} and {out_cty.name}")
    return out_mon, out_cty

def main():
    results = []
    for y in YEARS:
        mon_path, cty_path = clean_year(y)
        results.append({"year": y,
                        "monitor_trim": str(mon_path) if mon_path else None,
                        "county_agg": str(cty_path) if cty_path else None})
    pd.DataFrame(results).to_csv(OUT_DIR / "epa_conc_summary.csv", index=False)
    print("[EPA CONC] Done. Summary written to cleaned/epa_conc_summary.csv")

if __name__ == "__main__":
    main()
PY
