import glob
import pandas as pd
from pathlib import Path

# ---------------------------------------------
# Years & Output Directory
# ---------------------------------------------
YEARS = [2023, 2024]
OUT_DIR = Path("../cleaned_health_resources_data")
OUT_DIR.mkdir(exist_ok=True)

# ---------------------------------------------
# ID / GEO fields we will keep
# These are the AHRF variable names from the technical documentation.
# ---------------------------------------------
BASE_KEEP = [
    "fips_st_cnty",         # county FIPS
    "cnty_name",            # county name
    "st_name_abbrev",       # state abbreviation
    "cbsa_23",              # CBSA code (2023 release)
    "rural_urban_contnm_23" # Rural-Urban Continuum Code
]

ID_RENAME = {
    "fips_st_cnty": "fips",
    "cnty_name": "county",
    "st_name_abbrev": "state",
    "cbsa_23": "cbsa_code",
    "rural_urban_contnm_23": "rural_urban_code",
}

MEASURE_ALIASES = {

    "population": [
        "popn_est_22",
        "popn_est_21"
    ],

    # Primary Care MD per 100k
    "md_pc": [
        "md_nf_prim_care_pc_excl_rsdnt_22",
        "md_nf_prim_care_pc_excl_rsdnt_21"
    ],

    # Primary Care DO per 100k
    "do_pc": [
        "do_nf_prim_care_pc_excl_rsdnt_22",
        "do_nf_prim_care_pc_excl_rsdnt_21"
    ],

    # Physician Assistants (per 100k)
    "pa": [
        "pa_pc_22",
        "pa_pc_21"
    ],

    # Nurse Practitioners (per 100k)
    "np": [
        "np_pc_22",
        "np_pc_21"
    ],

    # APRN (per 100k)
    "aprn": [
        "aprn_pc_22",
        "aprn_pc_21"
    ],

    # NHSC Sites
    "nhsc_sites": [
        "nhsc_sites_24",
        "nhsc_sites_23"
    ],

    # NHSC FTE Primary Care
    "nhsc_fte_pc": [
        "nhsc_fte_prim_care_provdrs_24",
        "nhsc_fte_prim_care_provdrs_23",
        "nhsc_fte_provdrs_24",
        "nhsc_fte_provdrs_23"
    ],

    # Total Primary Care Clinicians per 100k
    "total_pc_per_100k": [
        "prim_care_phys_pc_22",
        "prim_care_phys_pc_21"
    ]
}

OUTPUT_COL_ORDER = [
    "fips", "state", "county",
    "cbsa_code", "rural_urban_code",
    "population",
    "md_pc", "do_pc", "pa", "np", "aprn",
    "nhsc_sites", "nhsc_fte_pc",
    "total_pc_per_100k",
    "year"
]


def read_csv_flexible(path):
    try:
        return pd.read_csv(path, low_memory=False)
    except UnicodeDecodeError:
        return pd.read_csv(path, low_memory=False, encoding="latin-1")

def clean_numeric(s):
    return pd.to_numeric(
        s.astype(str).str.replace(r"[,\s]", "", regex=True).replace({"": None}),
        errors="coerce"
    )

import numpy as np

def resolve_metric(df, alias_list, metric):
    for col in alias_list:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce")

    return pd.Series([np.nan] * len(df), dtype="float")

def find_files(year):
    return glob.glob(f"../health_resources/ahrf{year}*.csv")

def clean_file(path, year):
    raw = read_csv_flexible(path)

    present_ids = [c for c in BASE_KEEP if c in raw.columns]
    out = raw[present_ids].copy()
    out.rename(columns=ID_RENAME, inplace=True)

    # Format FIPS
    if "fips" in out.columns:
        out["fips"] = out["fips"].astype(str).str.zfill(5)

    for clean_name, aliases in MEASURE_ALIASES.items():
        out[clean_name] = resolve_metric(raw, aliases, clean_name)

    if "population" in out.columns:
        out["population"] = clean_numeric(out["population"])

    out["year"] = year

    ordered = [c for c in OUTPUT_COL_ORDER if c in out.columns]
    return out[ordered]

def main():
    summaries = []

    for year in YEARS:
        files = find_files(year)
        if not files:
            print(f"[AHRF] No raw file found for {year}")
            continue

        frames = [clean_file(f, year) for f in files]
        combined = pd.concat(frames, ignore_index=True)

        outpath = OUT_DIR / f"ahrf_cleaned_{year}.csv"
        combined.to_csv(outpath, index=False)

        print(f"[AHRF] Wrote {outpath.name} ({len(combined)} rows)")
        summaries.append({"year": year, "rows": len(combined)})

    pd.DataFrame(summaries).to_csv(OUT_DIR / "ahrf_summary.csv", index=False)
    print("[AHRF] Done.")

if __name__ == "__main__":
    main()
