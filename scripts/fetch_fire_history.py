import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv
from io import StringIO

load_dotenv()
API_KEY = os.getenv("NASA_API_KEY")

OUTPUT_DIR = "/opt/airflow/data"
FINAL_FILE = os.path.join(OUTPUT_DIR, "global_fires_history.csv")

# We use the Standard Product (MODIS_SP) which is scientifically quality-controlled
SOURCE = "MODIS_SP"
AREA = "world" # The Area API accepts 'world' as a shortcut for the whole globe
START_YEAR = 2001
END_YEAR = 2023

def fetch_fire_history():
    if not API_KEY:
        print("CRITICAL ERROR: NASA_API_KEY not found.")
        return

    all_years_data = []
    print(f"Starting Historical Scan ({START_YEAR}-{END_YEAR}) using Area API...")

    for year in range(START_YEAR, END_YEAR + 1):
        # We take a 1-day sample from the peak of fire season (August 1st)
        target_date = f"{year}-08-01"
        
        # URL Format: /api/area/csv/[KEY]/[SOURCE]/[AREA]/[DAYS]/[DATE]
        # We ask for 1 day of data starting on target_date
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{API_KEY}/{SOURCE}/{AREA}/1/{target_date}"
        
        try:
            print(f"Fetching global fires for {target_date}...", end=" ", flush=True)
            response = requests.get(url, timeout=60) # Give NASA time to process massive files
            
            if response.status_code != 200:
                print(f"[ERROR] API returned {response.status_code}")
                continue

            # The response is a big CSV string. 
            # If "No Data", the body might be short or just a header.
            csv_text = response.text
            
            # Simple check: If it's just a header, count is 0
            row_count = csv_text.strip().count('\n') 
            # Note: The first line is header, so actual fires = row_count (if > 0)
            
            if row_count > 0:
                print(f"-> Found {row_count} fires.")
                all_years_data.append({
                    "year": year,
                    "date": target_date,
                    "global_fire_count": row_count
                })
            else:
                print("-> 0 fires.")

            # Sleep to prevent rate limiting (Area API is heavy)
            time.sleep(1.5)

        except Exception as e:
            print(f"\n[FAILED] {year}: {e}")

    # Save Results
    if all_years_data:
        df = pd.DataFrame(all_years_data)
        df.to_csv(FINAL_FILE, index=False)
        print(f"\nSUCCESS: History saved to {FINAL_FILE}")
        print(df)
    else:
        print("\nFAILURE: No data collected.")

if __name__ == "__main__":
    fetch_fire_history()