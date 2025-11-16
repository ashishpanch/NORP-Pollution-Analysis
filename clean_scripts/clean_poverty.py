import argparse
import pandas as pd
from pathlib import Path

def clean_poverty(input_csv: str, output_csv: str):
    # Load data
    df = pd.read_csv(input_csv)

    # Keep only rows where Area_Name contains "county"
    is_county = df["Area_Name"].str.contains("county", case=False, na=False)

    # Keep only rows with attribute PCTPOVALL_2023 (case-insensitive)
    is_poverty = df["Attribute"].str.upper().eq("PCTPOVALL_2023")

    out = df[is_county & is_poverty].copy()

    # Normalize FIPS to 5-digit strings
    out["FIPS_Code"] = out["FIPS_Code"].astype("int64").astype(str).str.zfill(5)

    # Select and rename columns
    out = out[["FIPS_Code", "Area_Name", "Value"]]
    out = out.rename(columns={"Value": "Poverty Percentage"})

    # Save output
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_csv, index=False)


def main():
    parser = argparse.ArgumentParser(
        description="Clean PCTPOVALL_2023 poverty percentage data to county-level table."
    )
    parser.add_argument("input_csv", help="Path to raw CSV file")
    parser.add_argument(
        "-o",
        "--output",
        default="cleaned_poverty_percentage_2023.csv",
        help="Output CSV file (default: cleaned_poverty_percentage_2023.csv)",
    )
    args = parser.parse_args()

    clean_poverty(args.input_csv, args.output)


if __name__ == "__main__":
    main()
