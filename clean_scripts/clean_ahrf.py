import os, re, glob
import pandas as pd
from pathlib import Path

OUT_DIR = Path("cleaned_health_resources_data")
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
    hr_dir = Path("health_resources")
    if not hr_dir.exists():
        print("[AHRF] Directory 'health_resources' not found.")
        files = []
    else:
        # match files named either "ahrf_{year}.csv" or "ahrf{year}.csv"
        patterns = ("ahrf_*.csv", "ahrf*.csv")
        files = sorted({str(p) for pat in patterns for p in hr_dir.glob(pat)})
    if not files:
        print("[AHRF] No ahrf*.csv files found.")
        return
    summary = []
    for f in files:
        year = extract_year(os.path.basename(f))
        df = read_csv_flexible(f)
        df = ensure_fips(df)
        out = df.copy()
        out_path = OUT_DIR / f"ahrf_{year}_cleaned.csv"
        out.to_csv(out_path, index=False)
        summary.append({"file": f, "year": year, "rows": len(out), "out": str(out_path)})
        print(f"[AHRF] Wrote {out_path.name} ({len(out)} rows)")
    pd.DataFrame(summary).to_csv(OUT_DIR / "ahrf_summary.csv", index=False)
    all_out = pd.concat([pd.read_csv(row["out"], dtype=str, low_memory=False) for row in summary], ignore_index=True)
    all_out.to_csv(OUT_DIR / "ahrf_all_trim.csv", index=False)

    print("[AHRF] Done.")

if __name__ == "__main__":
    main()
