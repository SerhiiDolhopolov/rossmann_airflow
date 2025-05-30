import matplotlib.pyplot as plt

from config import SQL_PATH
from src.diagrams.diagram_generator import generate_diagram

SQL_PATH = SQL_PATH / "revenue_by_shop.sql"
TITLE = "Загальна сума доходу по магазинах"


def generate_revenue_by_shop(
    output_path: str, 
    from_date: str, 
    to_date: str = None,
    df_archive_name: str = None,
):
    generate_diagram(
        output_path=output_path,
        sql_path=SQL_PATH,
        plot_diagram_func=plot_revenue_by_shop,
        title=TITLE,
        from_date=from_date,
        to_date=to_date,
        df_archive_name=df_archive_name,
    )
    
    
def plot_revenue_by_shop(df) -> plt.Figure:
    group_columns = ['shop_id', 'country', 'city']
    if not df.empty:
        df = (
            df.groupby(group_columns, as_index=False)['total_revenue']
            .sum()
            .sort_values(by='total_revenue', ascending=False)
        )
    plt.figure(figsize=(10, 6))
    plt.axis('off')
    plt.table(
        cellText=df.round(2).values,
        colLabels=df.columns,
        loc='center',
        cellLoc='center'
    )
    return plt
