import logging

import pandas as pd

from src.clickhouse import get_clickhouse_client

logger = logging.getLogger(__name__)


def fetch_by_date_df(
    sql_path: str,
    from_date: str, 
    to_date: str,
) -> pd.DataFrame:
    try:
        with open(sql_path, "r") as file:
            query = file.read()
        query = query.format(
            start_date=from_date,
            end_date=to_date
        )
        with get_clickhouse_client() as client:
            df = client.query_df(query)
            return df
    except Exception as e:
        logging.error(
            "Error fetching orders by date: %s", 
            e, 
            exc_info=True
        )
        raise RuntimeError(f"Error fetching orders by date: {e}") from e