from sqlalchemy import create_engine
import pandas as pd
import os

# Database Connection
DB_CONN = "postgresql://airflow:airflow@postgres:5432/airflow"
OUTPUT_FILE = "/opt/airflow/data/gold/final_climate_fire_analysis.csv"

def run_transformation():
    print("--- Starting Data Transformation (Gold Layer) ---")
    
    try:
        engine = create_engine(DB_CONN)
        
        # 1. Aggregates raw fire data into annual counts and intensity metrics using date extraction.
        # 2. Joins these fire stats with temperature anomalies using a LEFT JOIN to ensure all years are represented.
        # 3. Filters for data from the year 2000 onward and saves the result into a clean summary table.
        sql_query = """
        CREATE TABLE IF NOT EXISTS annual_global_summary AS
        WITH fire_stats AS (
            -- Calculate annual fire metrics from raw data
            SELECT 
                EXTRACT(YEAR FROM TO_DATE(fire_date, 'YYYY-MM-DD')) as year,
                COUNT(*) as total_fires,
                AVG(intensity_mw) as avg_intensity,
                MAX(intensity_mw) as max_intensity
            FROM raw_fires
            GROUP BY 1
        ),
        temp_stats AS (
            -- Select relevant temperature columns
            SELECT 
                year,
                temp_anomaly_celsius
            FROM raw_temperatures
        )
        -- JOIN them together
        SELECT 
            t.year,
            t.temp_anomaly_celsius,
            COALESCE(f.total_fires, 0) as total_fires,
            COALESCE(f.avg_intensity, 0) as avg_intensity,
            COALESCE(f.max_intensity, 0) as max_intensity
        FROM temp_stats t
        LEFT JOIN fire_stats f ON t.year = f.year
        WHERE t.year >= 2000
        ORDER BY t.year DESC;
        """
        
        # 2. Execute the Query
        # We drop the table first to ensure a fresh build every time (Idempotency)
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS annual_global_summary")
            conn.execute(sql_query)
            
        print("SUCCESS: Created 'annual_global_summary' table.")
        
        # 3. Verify the Output
        # Read the new table back into Pandas to show you the results
        df = pd.read_sql("SELECT * FROM annual_global_summary", engine)

        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

        print("\n--- Final Analytics Data ---")
        print(df.head(25)) # Show all years since 2000
        
        # Save to CSV for easy dashboarding later (optional)
        df.to_csv("/opt/airflow/data/gold/final_climate_fire_analysis.csv", index=False)
        print("\nSaved copy to data/gold/final_climate_fire_analysis.csv")

    except Exception as e:
        print(f"TRANSFORMATION FAILED: {e}")

if __name__ == "__main__":
    run_transformation()