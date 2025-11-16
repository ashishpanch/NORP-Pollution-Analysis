import pandas as pd
import sys
import os
import glob

def clean_and_merge_migration_data(year_suffix):
    """
    Cleans and merges county inflow and outflow data for a given year range.
    Keeps only rows with 'Total Migration-US and Foreign' in the county name.
    """

    inflow_file = os.path.join("migration", "inflow", f"countyinflow{year_suffix}.csv")
    outflow_file = os.path.join("migration", "outflow", f"countyoutflow{year_suffix}.csv")

    if not os.path.exists(inflow_file) or not os.path.exists(outflow_file):
        print(f" Missing file(s) for {year_suffix}, skipping.")
        return None

    print(f"\n Loading inflow data: {inflow_file}")
    print(f" Loading outflow data: {outflow_file}")

    inflow = pd.read_csv(inflow_file, encoding="latin1")
    outflow = pd.read_csv(outflow_file, encoding="latin1")

    # ---- Validate columns ----
    print(f"Inflow columns: {list(inflow.columns)}")
    print(f"Outflow columns: {list(outflow.columns)}")

    # -----------------------------
    # Select and rename inflow columns
    # -----------------------------
    inflow = inflow.rename(columns={
        "y2_statefips": "statefips",
        "y2_countyfips": "countyfips",
        "y1_state": "state",
        "y1_countyname": "countyname",
        "n1": "n1_inflow",
        "n2": "n2_inflow",
        "agi": "agi_inflow"
    })

    outflow = outflow.rename(columns={
        "y1_statefips": "statefips",
        "y1_countyfips": "countyfips",
        "y2_state": "state",
        "y2_countyname": "countyname",
        "n1": "n1_outflow",
        "n2": "n2_outflow",
        "agi": "agi_outflow"
    })

    # -----------------------------
    # Keep only rows with "Total Migration-US and Foreign"
    # -----------------------------
    inflow = inflow[inflow["countyname"].str.contains("Total Migration-US and Foreign", case=False, na=False)]
    outflow = outflow[outflow["countyname"].str.contains("Total Migration-US and Foreign", case=False, na=False)]

    # -----------------------------
    # Merge inflow and outflow data
    # -----------------------------
    merged = pd.merge(
        inflow,
        outflow,
        on=["state", "countyname", "statefips", "countyfips"],
        how="inner"
    )

    # Compute net migration (inflow - outflow)
    merged["net_migration"] = merged["n1_inflow"] - merged["n1_outflow"]

    # Add FIPS (state + county)
    merged["fips"] = (
        merged["statefips"].astype(str).str.zfill(2)
        + merged["countyfips"].astype(str).str.zfill(3)
    )

    cleaned = merged[[
        "fips",
        "state",
        "countyname",
        "n1_inflow",
        "n1_outflow",
        "net_migration",
        "agi_inflow",
        "agi_outflow"
    ]]

    os.makedirs("cleaned_migration_data", exist_ok=True)
    output_file = os.path.join("cleaned_migration_data", f"cleaned_migration_{year_suffix}.csv")
    cleaned.to_csv(output_file, index=False)

    print(f" Cleaned and merged file saved to: {output_file}")
    print(f" Total counties processed: {len(cleaned)}")

    return cleaned


def clean_all_years():
    """Automatically finds and processes all inflow/outflow file pairs."""
    inflow_files = glob.glob(os.path.join("migration", "inflow", "countyinflow*.csv"))
    outflow_files = glob.glob(os.path.join("migration", "outflow", "countyoutflow*.csv"))

    inflow_years = {os.path.basename(f).replace("countyinflow", "").replace(".csv", "") for f in inflow_files}
    outflow_years = {os.path.basename(f).replace("countyoutflow", "").replace(".csv", "") for f in outflow_files}

    valid_years = sorted(inflow_years.intersection(outflow_years))
    if not valid_years:
        print(" No matching inflow/outflow file pairs found.")
        return

    print(f"\n Found {len(valid_years)} matching year pairs: {', '.join(valid_years)}")
    for year_suffix in valid_years:
        clean_and_merge_migration_data(year_suffix)


# -----------------------------
# Script entry point
# -----------------------------
if __name__ == "__main__":
    if len(sys.argv) == 2:
        arg = sys.argv[1]
        if arg.lower() == "all":
            clean_all_years()
        else:
            clean_and_merge_migration_data(arg)
    else:
        print("Usage:")
        print("  python clean_migration_data.py <year_suffix>")
        print("  python clean_migration_data.py all")
        print("\nExamples:")
        print("  python clean_migration_data.py 1516")
        print("  python clean_migration_data.py all")
