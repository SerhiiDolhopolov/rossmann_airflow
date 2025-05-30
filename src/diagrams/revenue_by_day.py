import matplotlib.pyplot as plt
import seaborn as sns

from config import SQL_PATH
from src.diagrams.diagram_generator import generate_diagram

SQL_PATH = SQL_PATH / "revenue_by_day.sql"
TITLE = "Динаміка доходу по днях"


def generate_revenue_by_day(
    output_path: str, 
    from_date: str, 
    to_date: str = None,
    df_archive_name: str = None,
):
    generate_diagram(
        output_path=output_path,
        sql_path=SQL_PATH,
        plot_diagram_func=plot_revenue_by_day,
        title=TITLE,
        from_date=from_date,
        to_date=to_date,
        df_archive_name=df_archive_name,
    )


def plot_revenue_by_day(df) -> plt.Figure:
    group_columns = ['day', 'shop_id', 'country', 'city']
    df = (
        df.groupby(group_columns, as_index=False)['total_revenue']
        .sum()
    )
    df['shop_info'] = (
        df['shop_id'].astype(str)
        + ' ('
        + df['country']
        + ' | '
        + df['city']
        + ')'
    )
    plt.figure(figsize=(10, 6))
    sns.barplot(
        x='day', 
        y='total_revenue', 
        hue='shop_info',
        data=df, 
        palette='Blues_d'
    )
    plt.xticks(rotation=45)
    plt.xlabel("День")
    plt.ylabel("Сума доходу")
    return plt