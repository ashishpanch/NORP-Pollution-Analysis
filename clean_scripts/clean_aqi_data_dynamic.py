import pandas as pd
import sys
import os

def clean_aqi_data(year, output_dir):
    """Clean annual AQI by county data for the given year."""
    input_file = os.path.join("aqi_by_county", f"annual_aqi_by_county_{year}.csv")
    output_file = os.path.join(output_dir, f"cleaned_aqi_{year}.csv")

    if not os.path.exists(input_file):
        print(f"[!] File not found for {year}: {input_file}")
        return

    # Load raw dataset
    aqi = pd.read_csv(input_file)

    # Keep only relevant columns
    aqi_clean = aqi[[
        "State",
        "County",
        "Year",
        "Days with AQI",
        "Good Days",
        "Moderate Days",
        "Unhealthy for Sensitive Groups Days",
        "Unhealthy Days",
        "Max AQI",
        "Median AQI"
    ]].copy()

    # Derived metrics
    aqi_clean["Pct_Good"] = aqi_clean["Good Days"] / aqi_clean["Days with AQI"]
    aqi_clean["Pct_Unhealthy"] = (
        (aqi_clean["Unhealthy for Sensitive Groups Days"] + aqi_clean["Unhealthy Days"])
        / aqi_clean["Days with AQI"]
    )

    # Clean up names
    aqi_clean["County"] = aqi_clean["County"].str.replace(" County", "", regex=False).str.strip().str.title()
    aqi_clean["State"] = aqi_clean["State"].str.title()

    # Save cleaned dataset
    aqi_clean.to_csv(output_file, index=False)

    print(f"[✓] Cleaned AQI dataset saved: {output_file}")


if __name__ == "__main__":
    # Create output folder if it doesn’t exist
    output_dir = "cleaned_aqi_data"
    os.makedirs(output_dir, exist_ok=True)

    if len(sys.argv) == 2:
        # Run for a specific year
        year = sys.argv[1]
        clean_aqi_data(year, output_dir)
    else:
        # Run for all years 2015–2025
        for year in range(2015, 2026):
            clean_aqi_data(year, output_dir)

        print("\nAll years processed and saved in 'cleaned_aqi_data/'")
