"""
Clean and combine IRS SOI county-to-county migration inflow/outflow (2016–2025 if available).
Expected filenames (any subset will be processed):
  countyinflowYYZZ.csv and countyoutflowYYZZ.csv  (e.g., countyinflow2122.csv for 2021–2022)

Output:
  cleaned/irs_migration_long.csv – long form by county, year_start, type (inflow/outflow)
  cleaned/irs_migration_wide.csv – wide per county-year with inflow/outflow + net
Notes:
 - Normalizes county FIPS (origin/destination as appropriate).
 - Computes net_migr = inflow_returns - outflow_returns (and AGI if available).
"""
import re, glob
import pandas as pd
from pathlib import Path

OUT_DIR = Path("cleaned")
OUT_DIR.mkdir(exist_ok=True)

# Column name guesses for IRS files (they vary by year)
FIPS_IN_CAND = ["dest_st","dest_st_cnty","DestStCnty","DestCountyFIPS","destination_fips","dst_fips"]
FIPS_OUT_CAND = ["origin_st","origin_st_cnty","OriginStCnty","OriginCountyFIPS","origin_fips","org_fips"]
RETURNS_CAND = ["n1","Number of returns","Number of Returns","returns"]
EXEMPTS_CAND = ["n2","Number of exemptions","exemptions"]
AGI_CAND = ["agi","AGI","adjusted_gross_income","agi_total"]

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def parse_years_from_name(name):
    m = re.search(r'(\d{2})(\d{2})', name)
    if not m:
        return None, None, None
    y1, y2 = int(m.group(1)), int(m.group(2))
    y1 += 2000 if y1 < 100 else 0
    y2 += 2000 if y2 < 100 else 0
    return f"{y1}-{y2}", y1, y2

def coalesce_col(df, cands, default=None):
    for c in cands:
        if c in df.columns:
            return df[c]
    return default

def normalize_fips(series):
    return series.astype(str).str.replace(r"\.0$","", regex=True).str.zfill(5)

def tidy(path, kind):
    df = read_csv_flexible(path)
    span, y1, y2 = parse_years_from_name(Path(path).name)
    df["_year_start"] = y1
    # inflow: DEST county receives households; outflow: ORIGIN county loses households
    if kind == "inflow":
        fips = coalesce_col(df, FIPS_IN_CAND, default="")
    else:
        fips = coalesce_col(df, FIPS_OUT_CAND, default="")
    df["_county_fips"] = normalize_fips(pd.Series(fips).fillna(""))
    df["_returns"] = pd.to_numeric(coalesce_col(df, RETURNS_CAND, default=0), errors="coerce").fillna(0)
    df["_exempts"] = pd.to_numeric(coalesce_col(df, EXEMPTS_CAND, default=0), errors="coerce").fillna(0)
    df["_agi"] = pd.to_numeric(coalesce_col(df, AGI_CAND, default=0), errors="coerce").fillna(0.0)
    out = df.groupby(["_county_fips","_year_start"], dropna=False).agg(
        returns=("_returns","sum"),
        exemptions=("_exempts","sum"),
        agi=("_agi","sum")
    ).reset_index().rename(columns={"_county_fips":"county_fips","_year_start":"year_start"})
    out["type"] = kind
    return out

def main():
    inflow_files = sorted(glob.glob("countyinflow*.csv"))
    outflow_files = sorted(glob.glob("countyoutflow*.csv"))
    long_frames = []

    for f in inflow_files:
        long_frames.append(tidy(f, "inflow"))
    for f in outflow_files:
        long_frames.append(tidy(f, "outflow"))

    if not long_frames:
        print("[IRS] No migration files found.")
        return

    long_all = pd.concat(long_frames, ignore_index=True)
    # Filter to requested span 2016–2025 starts
    long_all = long_all[(long_all["year_start"]>=2016) & (long_all["year_start"]<=2025)]
    out_long = OUT_DIR / "irs_migration_long.csv"
    long_all.to_csv(out_long, index=False)

    # Wide with net
    wide = long_all.pivot_table(index=["county_fips","year_start"],
                                columns="type",
                                values=["returns","exemptions","agi"],
                                aggfunc="sum").fillna(0)
    # Flatten columns
    wide.columns = ["_".join(col).strip() for col in wide.columns.values]
    wide = wide.reset_index()
    if {"returns_inflow","returns_outflow"}.issubset(wide.columns):
        wide["net_returns"] = wide["returns_inflow"] - wide["returns_outflow"]
    if {"agi_inflow","agi_outflow"}.issubset(wide.columns):
        wide["net_agi"] = wide["agi_inflow"] - wide["agi_outflow"]

    out_wide = OUT_DIR / "irs_migration_wide.csv"
    wide.to_csv(out_wide, index=False)

    print(f"[IRS] Wrote {out_long.name} ({len(long_all)} rows) and {out_wide.name} ({len(wide)} rows)")

if __name__ == "__main__":
    main()
PY
