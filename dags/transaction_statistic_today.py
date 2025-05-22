from datetime import timedelta
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

from src.s3 import get_transcation_files


default_args = {
    "owner": "airflow",
    "start_date": "2025-05-22",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

def compare_transcation_files():
    try:
        dfs = []
        files = get_transcation_files(1, 2025, 6, range(1, 8))
        if not files:
            print("No files found.")
            return
        
        for file in files:
            df = pd.read_csv(BytesIO(file.read()), encoding="utf-8")
            dfs.append(df)
    
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df.to_csv("week_transactions.csv", index=False)
        print(f"Files to compare: {files}")
    except Exception as e:
        print(f"Error fetching files: {e}")
        raise

with DAG(
    dag_id="transaction_statistic_today",
    default_args=default_args,
    schedule_interval="0 0 * * *",
) as dag:
    compare_transcation_files = PythonOperator(
        task_id="compare_transcation_files",
        python_callable=compare_transcation_files)
    compare_transcation_files