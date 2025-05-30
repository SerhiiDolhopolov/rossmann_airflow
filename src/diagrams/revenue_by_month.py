import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from config import SQL_PATH
from src.diagrams.diagram_generator import generate_diagram

SQL_PATH = SQL_PATH / "revenue_by_month.sql"
TITLE = "Динаміка доходу по місяцях"


def generate_revenue_by_month(
    output_path: str, 
    from_date: str, 
    to_date: str = None,
    df_archive_name: str = None,
):
    generate_diagram(
        output_path=output_path,
        sql_path=SQL_PATH,
        plot_diagram_func=plot_revenue_by_month,
        title=TITLE,
        from_date=from_date,
        to_date=to_date,
        df_archive_name=df_archive_name,
    )


def plot_revenue_by_month(df) -> plt.Figure:
    group_columns = ['month', 'shop_id', 'country', 'city']
    df['month'] = pd.to_datetime(df['month'])
    df = (
        df.groupby(group_columns, as_index=False)['total_revenue']
        .sum() \
        .sort_values(by=['month'], ascending=[True])
    )
    df['shop_info'] = (
        df['shop_id'].astype(str)
        + ' ('
        + df['country']
        + ' | '
        + df['city']
        + ')'
    )
    df['month'] = pd.to_datetime(df['month']).dt.strftime('%Y-%m')
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x='month', 
        y='total_revenue', 
        hue='shop_info',
        data=df, 
        palette='Blues_d'
    )
    plt.xticks(rotation=45)
    plt.xlabel("Місяць")
    plt.ylabel("Сума доходу")
    return plt