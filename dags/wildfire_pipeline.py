from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'wildfire_climate_pipeline',
    default_args=default_args,
    description='A pipeline to analyze the relationship between global warming and wildfire frequency.',
    schedule_interval='@yearly',
    start_date=days_ago(1),
    catchup=False,
    tags=['wildfire', 'climate', 'analysis','elt'],
) as dag:

    # Task 1: fetch_temperature 
    t1_fetch_temps = BashOperator(
        task_id='fetch_temperature',
        bash_command='python /opt/airflow/scripts/fetch_temperature.py'
    )

    # Task 2: fetch_wildfires_raw
    t2_fetch_fires = BashOperator(
        task_id='fetch_wildfires_raw',
        bash_command='python /opt/airflow/scripts/fetch_wildfires_raw.py'
    )

    # TASK 3: Load to Postgres
    t3_load_db = BashOperator(
        task_id='load_to_postgres',
        bash_command='python /opt/airflow/scripts/load_to_postgres.py',
    )

    # TASK 4: Transform (SQL) 
    t4_transform = BashOperator(
        task_id='transform_gold_layer',
        bash_command='python /opt/airflow/scripts/transform_data.py',
    )

    # TASK 5: Visualize 
    t5_visualize = BashOperator(
        task_id='generate_chart',
        bash_command='python /opt/airflow/scripts/visualize_trends.py',
    )

    # Define Task Dependencies

    #t1 and t2 can run in parallel since they are independent data fetches
    [t1_fetch_temps, t2_fetch_fires]>>t3_load_db
    # After loading, we transform the data
    t3_load_db>>t4_transform>>t5_visualize