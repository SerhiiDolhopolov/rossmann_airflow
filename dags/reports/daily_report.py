from datetime import datetime

from airflow import DAG

from config import TMP_PATH, SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL
from dags.reports.report_template import get_report_dag
from src.diagrams.revenue_by_shop import generate_revenue_by_shop
from src.diagrams.payment_distrib import generate_payment_distrib
from src.diagrams.products_quantity import generate_products_quantity
from src.s3 import upload_daily_report


diagrams = [
    {
        'task_id': 'revenue_by_shop',
        'python_callable': generate_revenue_by_shop,
        'output_path': TMP_PATH / "daily_revenue_by_shop.png",
        'image_size': (190, 190),
    },
    {
        'task_id': 'payment_distrib',
        'python_callable': generate_payment_distrib,
        'output_path': TMP_PATH / "daily_payment_distrib.png",
        'image_size': (190, 190),
    },
    {
        'task_id': 'products_quantity',
        'python_callable': generate_products_quantity,
        'output_path': TMP_PATH / "daily_products_quantity.png",
        'image_size': (190, 190),
    },
]


get_report_dag(
    dag_id="daily_report",
    schedule_interval="@daily",
    tags=["daily"],
    start_date=datetime(2025, 5, 1),
    from_date='{{ macros.ds_add(ds, -1) }}',
    to_date='{{ macros.ds_add(ds, -1) }}', 
    title="Daily Revenue Report",
    body=(
        "This report contains the daily revenue data, "
        "including revenue by shop, "
        "payment distribution, "
        "and product quantities sold."
    ),
    sender_email=SENDER_EMAIL,
    sender_password=SENDER_PASSWORD,
    recipient_email=RECIPIENT_EMAIL,
    diagrams=diagrams,
    upload_func=upload_daily_report,
)