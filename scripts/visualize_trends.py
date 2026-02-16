import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Configuration
INPUT_FILE = "/opt/airflow/data/gold/final_climate_fire_analysis.csv"
OUTPUT_IMG = "/opt/airflow/data/gold/climate_fire_correlation.png"

def plot_trends():
    print("--- Generating Correlation Chart ---")
    
    # 1. Read the Gold Data
    if not os.path.exists(INPUT_FILE):
        print("ERROR: Gold data file not found. Run transform_data.py first.")
        return
        
    df = pd.read_csv(INPUT_FILE).sort_values("year")
    
    # 2. Setup the Plot (Dual Axis)
    fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Plot Temperature (Red Line)
    color = 'tab:red'
    ax1.set_xlabel('Year', fontsize=12)
    ax1.set_ylabel('Global Temp Anomaly (°C)', color=color, fontsize=12)
    sns.lineplot(data=df, x='year', y='temp_anomaly_celsius', ax=ax1, color=color, marker='o', label='Temperature')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, alpha=0.3)

    # Create a second y-axis for Fires
    ax2 = ax1.twinx()
    color = 'tab:orange'
    ax2.set_ylabel('Total Global Fires (Millions)', color=color, fontsize=12)
    # We divide by 1,000,000 to make the axis cleaner
    sns.barplot(data=df, x='year', y='total_fires', ax=ax2, color=color, alpha=0.3, label='Fire Count')
    ax2.tick_params(axis='y', labelcolor=color)

    # Title & Layout
    plt.title('Global Warming vs. Wildfire Frequency (2000-2023)', fontsize=16, pad=20)
    fig.tight_layout()
    
    # 3. Save
    plt.savefig(OUTPUT_IMG)
    print(f"SUCCESS: Chart saved to {OUTPUT_IMG}")
    print("Go check your 'data/gold' folder on your laptop!")

if __name__ == "__main__":
    plot_trends()