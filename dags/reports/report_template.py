from datetime import datetime, timedelta
from typing import Callable

from airflow import DAG
from airflow.operators.python import PythonOperator

from config import TMP_PATH
from src.pdf import create_pdf_report
from email_utils import send_email_report

default_args = {
    "owner": "airflow",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}


def get_report_dag(
    dag_id: str,
    schedule_interval: str,
    start_date: datetime,
    from_date: str,
    to_date: str,
    title: str,
    body: str,
    sender_email: str,
    sender_password: str,
    recipient_email: str,
    diagrams: list[dict], 
    catchup: bool = False,
    tags: list[str] | None = None,
    upload_func: Callable | None = None,
) -> DAG:
    with DAG(
        dag_id=dag_id,
        tags=["report", *tags] or ["report"],
        default_args=default_args,
        schedule_interval=schedule_interval,
        start_date=start_date,
        catchup=catchup,
        max_active_runs=1,
    ) as dag:
        diagram_tasks = []
        diagram_images = dict()
        for diagram in diagrams:
            output_path = TMP_PATH / f"{start_date.strftime('%Y-%m-%d')}_{diagram['output_file_name']}"
            df_archive_name = diagram.get('df_archive_name', None)
            diagram_tasks.append(
                PythonOperator(
                    task_id=diagram['task_id'],
                    python_callable=diagram['python_callable'],
                    op_kwargs={
                        "output_path": output_path,
                        "from_date": from_date,
                        "to_date": to_date,
                        "df_archive_name": df_archive_name,
                    },
                )
            )
            diagram_images[output_path] = diagram.get('image_size', (190, 190))
        
        pdf_path = TMP_PATH / f"{start_date}_{dag_id}.pdf"
        total_pdf_task = PythonOperator(
                task_id="total_pdf",
                dag=dag,
                python_callable=create_pdf_report,
                op_kwargs={
                    "title": title,
                    "output_file_name": pdf_path,
                    "images": diagram_images,
                },
            )
        
        email_message_task = PythonOperator(
                task_id="send_email_report",
                dag=dag,
                python_callable=send_email_report,
                op_kwargs={
                    "sender_email": sender_email,
                    "sender_password": sender_password,
                    "recipient_email": recipient_email,
                    "subject": title,
                    "body": body,
                    "email_attachments": [
                        pdf_path,
                    ],
                }
            )
        
        out_tasks = [email_message_task]
        if upload_func:
            upload_task = PythonOperator(
                task_id="upload_report",
                dag=dag,
                python_callable=upload_func,
                op_kwargs={
                    "file_path": pdf_path,
                    "date": from_date,
                },
            )
            out_tasks.append(upload_task)
        
        (
            diagram_tasks
            >> total_pdf_task 
            >> out_tasks
        )
    return dag