import pandas as pd
import os
from sqlalchemy import create_engine # Python library for connecting to databases
from glob import glob

# --- CONFIGURATION ---
# Connection String: postgresql://[user]:[password]@[host]:[port]/[database]
# Use 'postgres' as the host since Airflow and Postgres are in the same Docker network
DB_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"

#File paths
DATA_DIR = "/opt/airflow/data"
TEMP_FILE = os.path.join(DATA_DIR, "global_temps_clean.csv")
FIRE_DIR = os.path.join(DATA_DIR, "bronze/fires")

def load_data():
    print("Loading data into PostgreSQL...")

    #Establish connection to PostgreSQL
    try:
        engine = create_engine(DB_CONN)
        connection = engine.connect()
        print("Connected to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return
    
    #Load global temperatures data
    try:
        if os.path.exists(TEMP_FILE):
            print(f"Loading global temperatures data from {TEMP_FILE}...")
            df_temp = pd.read_csv(TEMP_FILE)

            #Write to PostgreSQL
            # If the table already exists, replace it
            df_temp.to_sql('raw_temperatures', con=engine, if_exists='replace', index=False)
            print("Global temperatures data loaded successfully.")
        else:
            print(f"File {TEMP_FILE} not found. Skipping global temperatures data.")
    except Exception as e:
        print(f"Error loading global temperatures data: {e}")


    #Load fire data
    try:
        print(f"Loading fire data from {FIRE_DIR}...")

        # 1-Find all CSV files in subfolders
        # glob pattern to find all CSV files in subdirectories

        fire_files = glob(f"{FIRE_DIR}/**/*.csv") 

        if not fire_files:
            print(f"No fire data files found in {FIRE_DIR}. Skipping fire data.")
            return

        total_rows = 0

        #Loop through the files and append to database
        for i, file_path in enumerate(fire_files):
            df_fire = pd.read_csv(file_path)

            # Optimization: We only need specific columns for analytics
            # This saves space in the database
            keep_columns = ['latitude', 'longitude', 'acq_date', 'frp', 'confidence']

            # Check if columns exist (API formats can change slightly)
            available_cols = [c for c in keep_columns if c in df_fire.columns]
            df_fire = df_fire[available_cols]

            # Rename for clarity
            # acq_date -> fire_date, frp -> intensity_mw
            df_fire.rename(columns={'acq_date': 'fire_date', 'frp': 'intensity_mw'}, inplace=True)

            # Write to SQL
            # if_exists='append': Adds to the existing table instead of overwriting
            # if_exists='replace': Only for the FIRST file
            mode = 'replace' if i == 0 else 'append'

            df_fire.to_sql('raw_fires', engine, if_exists=mode, index=False)

            rows = len(df_fire)
            total_rows += rows
            print(f" -> Loaded {os.path.basename(file_path)} ({rows} rows)")

        print(f"Fire data loaded successfully. Total rows: {total_rows}")

    except Exception as e:
        print(f"Error loading fire data: {e}")


if __name__ == "__main__":    load_data()        