import os
import pandas as pd

INPUT_DIR = "concentration_counties"
OUTPUT_DIR = "cleaned_concentrations_by_monitor"

# Pollutants we want to keep
PM25_KEYWORDS = ["PM2.5 - Local Conditions"]
OZONE_KEYWORDS = ["Ozone"]

# Valid sample durations per pollutant
VALID_DURATIONS = {
    "PM2.5": ["24 HOUR"],
    "Ozone": ["8-HR RUN AVG BEGIN HOUR"]
}

# Columns to keep
KEEP_COLS = [
    "County Code",
    "State Name", "County Name", "City Name",
    "Parameter Name", "Sample Duration", "Units of Measure",
    "Arithmetic Mean", "Arithmetic Standard Dev", "1st Max Value",
    "2nd Max Value","3rd Max Value", "4th Max Value",
    "Observation Count", "Observation Percent", "Completeness Indicator"
]

def is_pm25(parameter):
    return any(k in parameter for k in PM25_KEYWORDS)

def is_ozone(parameter):
    return parameter == "Ozone"

def clean_single_file(filepath):
    print(f"Loading: {filepath}")

    df = pd.read_csv(filepath, encoding="latin1")

    # Keep only relevant columns
    cols_to_use = [c for c in KEEP_COLS if c in df.columns]
    df = df[cols_to_use].copy()

    # Filter pollutants
    df = df[
        df["Parameter Name"].apply(lambda x: is_pm25(x) or is_ozone(x))
    ]

    # Apply valid sample durations
    def valid_duration(row):
        pname = row["Parameter Name"]
        pdur = row["Sample Duration"]

        if is_pm25(pname):
            return pdur in VALID_DURATIONS["PM2.5"]

        if is_ozone(pname):
            return pdur in VALID_DURATIONS["Ozone"]

        return False

    df = df[df.apply(valid_duration, axis=1)]

    return df


def clean_all_years():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    files = [
        f for f in os.listdir(INPUT_DIR)
        if f.startswith("annual_conc_by_monitor_") and f.endswith(".csv")
    ]

    if not files:
        print("No pollution files found.")
        return

    for f in files:
        year = f.split("_")[-1].replace(".csv", "")
        in_path = os.path.join(INPUT_DIR, f)
        out_path = os.path.join(OUTPUT_DIR, f"air_cleaned_{year}.csv")

        print(f"\nCleaning year {year}...")

        cleaned_df = clean_single_file(in_path)
        cleaned_df.to_csv(out_path, index=False)

        print(f"Saved: {out_path}")


if __name__ == "__main__":
    clean_all_years()
