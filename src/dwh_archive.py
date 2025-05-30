import logging
from datetime import date

from src.clickhouse import get_clickhouse_client
from src.s3 import get_archive_transactions_file_path
from src.s3 import get_archive_deliveries_file_path
from config import SQL_PATH
from config import S3_ACCESS_KEY, S3_SECRET_KEY

logger = logging.getLogger(__name__)


def archive_transactions(
    month_date: str,
) -> None:
    month_date = date.fromisoformat(month_date)
    sql_path = SQL_PATH / "archive_transactions.sql"
    s3_file_path = get_archive_transactions_file_path(month_date)
    archive(month_date, sql_path, s3_file_path)
    
    
def archive_deliveries(
    month_date: str,
) -> None:
    month_date = date.fromisoformat(month_date)
    sql_path = SQL_PATH / "archive_deliveries.sql"
    s3_file_path = get_archive_deliveries_file_path(month_date)
    archive(month_date, sql_path, s3_file_path)


def archive(month_date: date, sql_path: str, s3_file_path: str) -> None:
    try:
        month_start_str = month_date.replace(day=1).strftime('%Y-%m-%d')
        next_month_start = get_next_month_start(month_date).strftime('%Y-%m-%d')
        
        with open(sql_path, "r") as file:
            query = file.read()
        query = query.format(
            s3_file_path=s3_file_path,
            s3_access_key=S3_ACCESS_KEY,
            s3_secret_key=S3_SECRET_KEY,
            month_start=month_start_str,
            next_month_start=next_month_start
        )
        with get_clickhouse_client() as client:
            client.command(query)
    except Exception as e:
        logger.error(
            "Error exporting to parquet: %s", 
            e, 
            exc_info=True
        )
        raise
    
    
def get_next_month_start(month: date) -> date:
    if month.month == 12:
        return date(month.year + 1, 1, 1)
    else:
        return date(month.year, month.month + 1, 1)