from datetime import timedelta
import csv

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.clickhouse import get_clickhouse_client


default_args = {
    "owner": "airflow",
    "start_date": "2025-05-22",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

def get_today_orders():
    try:
        with get_clickhouse_client() as client:
            result = client.query('SELECT * FROM delivery.reports')
            with open('table_output.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(result.column_names)
                for row in result.result_rows:
                    writer.writerow(row)
    except Exception as e:
        print(f"Error fetching orders: {e}")
        raise

with DAG(
    dag_id="delivery_statistic_today",
    default_args=default_args,
    schedule_interval="0 0 * * *",
) as dag:
    read_today_orders_task = PythonOperator(
        task_id="read_today_orders",
        python_callable=get_today_orders)
    
    read_today_orders_task 
    