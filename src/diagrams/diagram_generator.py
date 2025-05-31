import os
import logging
from typing import Callable
from datetime import date
from dateutil.relativedelta import relativedelta

import pandas as pd
import matplotlib.pyplot as plt

from diagrams.query_by_date import fetch_by_date_df
from src.s3 import get_archive_df, upload_archive_df

logger = logging.getLogger(__name__)


def generate_diagram(
    output_path: str, 
    sql_path: str,
    plot_diagram_func: Callable[[pd.DataFrame], plt.Figure],
    title: str,
    from_date: str, 
    to_date: str = None,
    df_archive_name: str = None,
):
    if not to_date:
        to_date = from_date
    title = (
        title
        + " "
        + from_date
        + (f" - {to_date}" if from_date != to_date else '')
    )
    
    dfs_from_s3 = []
    loading_from_s3 = False
    start_d = date.fromisoformat(from_date)
    end_d = date.fromisoformat(to_date)
    if end_d - relativedelta(months=3) > start_d:
        loading_from_s3 = True
        archive_end = (end_d - relativedelta(months=3)).replace(day=1)
        if df_archive_name:
            dfs_from_s3 = get_archive_dfs(
                df_archive_name=df_archive_name,
                start_d=start_d,
                archive_end=archive_end
            )
        from_date = archive_end.strftime('%Y-%m-%d')
        
    df = pd.concat(dfs_from_s3) if dfs_from_s3 else pd.DataFrame()
    df = pd.concat([df, fetch_by_date_df(sql_path, from_date, to_date)])
    df = df.drop_duplicates()
    if df.empty:
        logger.warning(
            "No data available for the period %s to %s.",
            from_date,
            to_date,
        )
        raise RuntimeError(f"No data available for the period {from_date} to {to_date}.")
    
    figure = plot_diagram_func(df)
    figure.title(title)
    figure.tight_layout()
    figure.savefig(output_path)
    if not loading_from_s3:  
        if df_archive_name:
            upload_archive_df(
                df_name=f"{df_archive_name}_{from_date}_{to_date}",
                df=df,
            )
            

def get_archive_dfs(
    df_archive_name: str,
    start_d: date,
    archive_end: date,
) -> list[pd.DataFrame]:
    dfs_from_s3 = []
    current_d = start_d
    while current_d < archive_end:
            next_month = (current_d + relativedelta(months=1))
            archive_to = next_month - relativedelta(days=1)
            current_d_str = current_d.strftime('%Y-%m-%d')
            archive_to_str = archive_to.strftime('%Y-%m-%d')
            try:
                dfs_from_s3.append(
                    get_archive_df(
                        df_name=f"{df_archive_name}_{current_d_str}_{archive_to_str}",
                    )
                )
            except Exception as e:
                logger.error(
                    "Error loading archive data for period %s to %s: %s",
                    current_d_str,
                    archive_to_str,
                    e,
                )
            current_d = next_month
    return dfs_from_s3