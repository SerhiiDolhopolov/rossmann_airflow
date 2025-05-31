from datetime import datetime

from airflow import DAG

from config import SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL
from dags.reports.report_template import get_report_dag
from src.diagrams.revenue_by_shop import generate_revenue_by_shop
from src.diagrams.revenue_by_month import generate_revenue_by_month
from src.diagrams.payment_distrib import generate_payment_distrib
from src.diagrams.products_quantity import generate_products_quantity
from src.s3 import upload_monthly_report


diagrams = [
    {
        'task_id': 'revenue_by_shop',
        'python_callable': generate_revenue_by_shop,
        'output_file_name': "monthly_revenue_by_shop.png",
        'image_size': (190, 190),
        'df_archive_name': 'monthly_revenue_by_shop',
    },
    {
        'task_id': 'revenue_by_month',
        'python_callable': generate_revenue_by_month,
        'output_file_name': "monthly_revenue_by_month.png",
        'image_size': (190, 190),
        'df_archive_name': 'monthly_revenue_by_month',
    },
    {
        'task_id': 'payment_distrib',
        'python_callable': generate_payment_distrib,
        'output_file_name':  "monthly_payment_distrib.png",
        'image_size': (190, 190),
        'df_archive_name': 'monthly_payment_distrib',
    },
    {
        'task_id': 'products_quantity',
        'python_callable': generate_products_quantity,
        'output_file_name': "monthly_products_quantity.png",
        'image_size': (190, 190),
        'df_archive_name': 'monthly_products_quantity',
    },
]


get_report_dag(
    dag_id="monthly_report",
    schedule_interval="@monthly",
    tags=["monthly"],
    start_date=datetime(2025, 1, 1),
    catchup=True,
    from_date="{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y-%m-01') }}",
    to_date="{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime('%Y-%m-%d') }}",
    title="Monthly Revenue Report",
    body=(
        "This report contains the montly revenue data, "
        "including revenue by shop, "
        "payment distribution, "
        "and product quantities sold."
    ),
    sender_email=SENDER_EMAIL,
    sender_password=SENDER_PASSWORD,
    recipient_email=RECIPIENT_EMAIL,
    diagrams=diagrams,
    upload_func=upload_monthly_report,
)