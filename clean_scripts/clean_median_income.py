import argparse
import pandas as pd
from pathlib import Path

def clean_median_income(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    # keep only county rows
    is_county = df["Area_Name"].str.contains("county", case=False, na=False)

    # keep only median household income 2022 rows (case-insensitive match)
    is_mhi_2022 = df["Attribute"].str.lower().eq("median_household_income_2022")

    out = df[is_county & is_mhi_2022].copy()

    # normalize FIPS to 5-digit string
    out["FIPS_Code"] = out["FIPS_Code"].astype("int64").astype(str).str.zfill(5)

    # keep / rename columns
    out = out[["FIPS_Code", "Area_Name", "Value"]]
    out = out.rename(columns={"Value": "Median Income"})

    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)

def main():
    parser = argparse.ArgumentParser(
        description="Clean median household income CSV to county-level table."
    )
    parser.add_argument("input_csv", help="Path to raw CSV file")
    parser.add_argument(
        "-o",
        "--output",
        default="cleaned_median_income_2022_county.csv",
        help="Path to output cleaned CSV (default: cleaned_median_income_2022_county.csv)",
    )
    args = parser.parse_args()
    clean_median_income(args.input_csv, args.output)

if __name__ == "__main__":
    main()
