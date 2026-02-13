#--- Docker Run Command ---
#--- docker-compose run --rm scheduler python scripts/fetch_temperature.py ---
#--- Comand info ---
#--- docker-compose run: Tells Docker to start a specific service defined in your YAML file. ---
#--- --rm: Automatically remove the container after it finishes (keeps your system clean). ---
#--- scheduler: Use the "scheduler" service because it has Python installed. ---
#--- python scripts/fetch_temperature.py: The command to execute inside the container. ---



import requests  # Library to send HTTP requests (like a browser)
import pandas as pd
import os

# --- CONFIGURATION ---
# The URL for NASA's Global Surface Temperature Analysis (GISTEMP v4)
# "Ts+dSST" means Surface Air Temperature + Sea Surface Temperature.
DATA_URL = "https://data.giss.nasa.gov/gistemp/tabledata_v4/GLB.Ts+dSST.csv"

OUTPUT_DIR = "/opt/airflow/data" 
RAW_FILE = os.path.join(OUTPUT_DIR, "global_temps_raw.csv")
CLEAN_FILE = os.path.join(OUTPUT_DIR, "global_temps_clean.csv")

def fetch_temperature_data():
    """
    Downloads and cleans global temperature anomaly data.
     - Fetches the CSV data from NASA's GISTEMP.
     - Saves the raw CSV to disk.
     - Cleans the data by removing metadata rows and converting to numeric.
     - Saves the cleaned data to disk.
     - Returns the cleaned DataFrame.
    """
    try:
        # Step 1: Fetch the data from NASA
        print("Fetching temperature data from NASA...")
        response = requests.get(DATA_URL)
        response.raise_for_status()  # Check if the request was successful

        # Step 2: Save the raw CSV to disk
        with open(RAW_FILE, "w") as f:
            f.write(response.text)
        print(f"Raw data saved to {RAW_FILE}")

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

    # Step 3: Load the CSV into a DataFrame and clean it
    try:
        # 1. Read the CSV into a Pandas DataFrame
        print("Cleaning the data...")
        df = pd.read_csv(RAW_FILE, skiprows=1)  # Skip the first row of metadata
        # 2. Filter Columns: Keep only 'Year' and 'J-D' (January to December average)
        clean_df = df[["Year", "J-D"]]
        # 3. Rename Columns for clarity
        clean_df.rename(columns={"Year": "year", "J-D": "temp_anomaly_celsius"}, inplace=True)
        # 4. Convert 'temp_anomaly_celsius' to numeric, coercing errors to NaN
        clean_df["temp_anomaly_celsius"] = pd.to_numeric(clean_df["temp_anomaly_celsius"], errors="coerce")
        # 5. Drop rows with NaN values
        clean_df.dropna(inplace=True)
        # 6. Save the cleaned data to disk
        clean_df.to_csv(CLEAN_FILE, index=False)
        print(f"Cleaned data saved to {CLEAN_FILE}")
        print("First 5 rows of cleaned data:")
        print(clean_df.head())

    except Exception as e:
        print(f"Error processing data: {e}")
        return None    

if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)  # Ensure the output directory exists

    fetch_temperature_data()