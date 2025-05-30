from datetime import datetime

from airflow import DAG

from config import TMP_PATH, SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAIL
from dags.reports.report_template import get_report_dag
from src.diagrams.revenue_by_shop import generate_revenue_by_shop
from src.diagrams.revenue_by_month import generate_revenue_by_month
from src.diagrams.payment_distrib import generate_payment_distrib
from src.diagrams.products_quantity import generate_products_quantity

diagrams = [
    {
        'task_id': 'revenue_by_shop',
        'python_callable': generate_revenue_by_shop,
        'output_path': TMP_PATH / "quarter_revenue_by_shop.png",
        'image_size': (190, 190),
    },
    {
        'task_id': 'revenue_by_month',
        'python_callable': generate_revenue_by_month,
        'output_path': TMP_PATH / "quarter_revenue_by_month.png",
        'image_size': (190, 190),
    },
    {
        'task_id': 'payment_distrib',
        'python_callable': generate_payment_distrib,
        'output_path': TMP_PATH / "quarter_payment_distrib.png",
        'image_size': (190, 190),
    },
    {
        'task_id': 'products_quantity',
        'python_callable': generate_products_quantity,
        'output_path': TMP_PATH / "quarter_products_quantity.png",
        'image_size': (190, 190),
    },
]


get_report_dag(
    dag_id="quarter_report",
    schedule_interval="@quarterly",
    tags=["quarterly"],
    start_date=datetime(2025, 5, 1),
    from_date="{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=3)).strftime('%Y-%m-01') }}",
    to_date="{{ ((execution_date - macros.dateutil.relativedelta.relativedelta(months=3)).replace(day=1) + macros.dateutil.relativedelta.relativedelta(months=3, days=-1)).strftime('%Y-%m-%d') }}", 
    title="Quarter Revenue Report",
    body=(
        "This report contains the quarter revenue data, "
        "including revenue by shop, "
        "payment distribution, "
        "and product quantities sold."
    ),
    sender_email=SENDER_EMAIL,
    sender_password=SENDER_PASSWORD,
    recipient_email=RECIPIENT_EMAIL,
    diagrams=diagrams
)