from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.dwh_archive import archive_transactions
from src.dwh_archive import archive_deliveries

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="dwh_archive",
    schedule_interval="@monthly",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    tags=["dwh", "datalake", "monthly"],
    catchup=True,
) as dag:
    archive_trans_task = PythonOperator(
        task_id='transactions',
        python_callable=archive_transactions,
        op_kwargs={
            "month_date": "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m-01') }}",
        },
        provide_context=True,
    )
    archive_deliveries_task = PythonOperator(
        task_id='deliveries',
        python_callable=archive_deliveries,
        op_kwargs={
            "month_date": "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m-01') }}",
        },
        provide_context=True,
    )
    [archive_trans_task, archive_deliveries_task]