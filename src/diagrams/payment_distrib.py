import matplotlib.pyplot as plt

from config import SQL_PATH
from src.diagrams.diagram_generator import generate_diagram


SQL_PATH = SQL_PATH / "payment_distrib.sql" 
TITLE = "Розподіл доходу по типам розрахунків"


def generate_payment_distrib(
    output_path: str, 
    from_date: str, 
    to_date: str = None,
    df_archive_name: str = None,
):
    generate_diagram(
        output_path=output_path,
        sql_path=SQL_PATH,
        plot_diagram_func=plot_payment_distrib,
        title=TITLE,
        from_date=from_date,
        to_date=to_date,
        df_archive_name=df_archive_name,
    )
    
    
def plot_payment_distrib(df) -> plt.Figure:
    df = (df
          .groupby('payment_method', as_index=True)['total_revenue']
          .sum()
          
    )
    plt.figure(figsize=(8, 8))
    plt.pie(df.values, labels=df.index, autopct='%1.1f%%', startangle=140)
    return plt