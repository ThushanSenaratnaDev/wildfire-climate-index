import requests
import os
import time
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.getenv("NASA_API_KEY")

# 2. Configuration
BASE_OUTPUT_DIR = "/opt/airflow/data/bronze/fires"
SOURCE = "MODIS_SP"  # Standard Product (Science Quality)
AREA = "world"       # Global coverage

# Grab data from 2000 to 2023
START_YEAR = 2000
END_YEAR = 2023

def fetch_raw_fire_data():
    if not API_KEY:
        print("CRITICAL ERROR: NASA_API_KEY not found.")
        return

    print(f"--- Starting Raw Data Ingestion: Global Fires ({START_YEAR}-{END_YEAR}) ---")
    print(f"Storage Path: {BASE_OUTPUT_DIR}")

    total_files = 0
    
    # Loop through every year
    for year in range(START_YEAR, END_YEAR + 1):
        # Create a sub-folder for this year (e.g., data/bronze/fires/2000)
        year_dir = os.path.join(BASE_OUTPUT_DIR, str(year))
        os.makedirs(year_dir, exist_ok=True)

        # Target Date: Peak Fire Season (August 1st)
        # Grab the full 24-hour window for the whole planet
        target_date = f"{year}-08-01"
        
        # Define the output filename
        file_name = f"fires_world_{target_date}.csv"
        file_path = os.path.join(year_dir, file_name)
        
        # Check if file already exists (Don't re-download if we have it)
        if os.path.exists(file_path):
            print(f"[SKIP] {year}: File already exists.")
            continue

        # Construct API URL
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{API_KEY}/{SOURCE}/{AREA}/1/{target_date}"
        
        try:
            print(f"Downloading {target_date}...", end=" ", flush=True)
            
            # Request data (Stream it to handle large files efficiently)
            with requests.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                
                # Check if API returned an error message instead of CSV
                if "Map Key not found" in r.text:
                    print("\n[ERROR] Invalid API Key.")
                    break
                
                # Write the raw content to disk
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            # Verify file size (Empty files usually mean no data or error)
            file_size_kb = os.path.getsize(file_path) / 1024
            print(f"-> Saved ({file_size_kb:.1f} KB)")
            total_files += 1

            # Wait 2 seconds between heavy requests
            time.sleep(2)

        except Exception as e:
            print(f"\n[FAILED] {year}: {e}")

    print(f"\n--- Job Complete. Downloaded {total_files} new files. ---")

if __name__ == "__main__":
    fetch_raw_fire_data()