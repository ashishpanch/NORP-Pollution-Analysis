"""
Clean HRSA AHRF county-level extract (flexible, uses whatever year is encoded in file).
Expected input filenames (any that exist will be processed):
  ahrf{YEAR}.csv  (e.g., ahrf2024.csv)

Output:
  cleaned/ahrf_{YEAR}_trim.csv – trimmed with county_fips + preserves all columns
Notes:
 - AHRF schema varies. This script keeps all columns but ensures a county_fips column.
 - If both 'FIPS' and 'F00002' exist, prefers the 5-digit code.
"""
import os, re, glob
import pandas as pd
from pathlib import Path

OUT_DIR = Path("cleaned")
OUT_DIR.mkdir(exist_ok=True)

def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def extract_year(name):
    m = re.search(r'ahrf(\d{4})', name)
    return m.group(1) if m else "unknown"

def ensure_fips(df):
    # Try common AHRF FIPS fields
    candidates = ["FIPS","fips","fipscounty","CountyFIPS","F00002"]
    for c in candidates:
        if c in df.columns:
            val = df[c].astype(str).str.replace(r"\.0$","", regex=True).str.zfill(5)
            df["county_fips"] = val
            return df
    # Split FIPS components if available
    if "State" in df.columns and "County" in df.columns:
        s = df["State"].astype(str).str.zfill(2)
        c = df["County"].astype(str).str.zfill(3)
        df["county_fips"] = s + c
        return df
    df["county_fips"] = None
    return df

def main():
    files = glob.glob("ahrf*.csv")
    if not files:
        print("[AHRF] No ahrf*.csv files found.")
        return
    summary = []
    for f in files:
        year = extract_year(os.path.basename(f))
        df = read_csv_flexible(f)
        df = ensure_fips(df)
        out = df.copy()
        out_path = OUT_DIR / f"ahrf_{year}_trim.csv"
        out.to_csv(out_path, index=False)
        summary.append({"file": f, "year": year, "rows": len(out), "out": str(out_path)})
        print(f"[AHRF] Wrote {out_path.name} ({len(out)} rows)")
    pd.DataFrame(summary).to_csv(OUT_DIR / "ahrf_summary.csv", index=False)
    print("[AHRF] Done.")

if __name__ == "__main__":
    main()
PY
