import matplotlib.pyplot as plt
import seaborn as sns

from config import SQL_PATH
from src.diagrams.diagram_generator import generate_diagram

SQL_PATH = SQL_PATH / "products_quantity.sql" 
TITLE = "Топ-20 продуктів за кількістю продажів"


def generate_products_quantity(
    output_path: str, 
    from_date: str, 
    to_date: str = None,
    df_archive_name: str = None,
):
    generate_diagram(
        output_path=output_path,
        sql_path=SQL_PATH,
        plot_diagram_func=plot_products_quantity,
        title=TITLE,
        from_date=from_date,
        to_date=to_date,
        df_archive_name=df_archive_name,
    )


def plot_products_quantity(df) -> plt.Figure:
    df = (df
        .groupby(['product_name', 'category_name'], as_index=False)['quantity']
        .sum()
        .sort_values(by='quantity', ascending=False)
    )
    plt.figure(figsize=(12, 8))
    sns.barplot(
        y='product_name', 
        x='quantity', 
        data=df, 
        hue='category_name', 
        dodge=False
    )
    plt.ylabel("")
    plt.xlabel("Кількість")
    return plt