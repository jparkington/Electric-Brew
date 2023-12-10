import matplotlib.pyplot as plt
import pandas as pd

from analysis.jp.flat import prepare_data
from utils.runtime    import find_project_root

def eda1(df: pd.DataFrame):
    '''
    Plots a scatter chart to visualize the relationship between kWh and Total Cost.

    Methodology:
        1. Create a scatter plot with 'kwh' on the x-axis and 'total_cost' on the y-axis.
        2. Use color to represent 'total_cost' and add a color bar for reference.

    Parameters:
        df (pd.DataFrame): The dataframe containing 'kwh' and 'total_cost' columns.

    Produces:
        A scatter plot saved as a PNG file and displayed on the screen.
    '''

    # 1: Creating the scatter plot with 'kwh' vs 'total_cost'
    plt.scatter(x = df['kwh'], 
                y = df['total_cost'],
                c = df['total_cost'],
                cmap = 'viridis',
                edgecolor = None)

    # 2: Adding a color bar to represent 'total_cost'
    plt.colorbar(label = 'Total Cost')

    # Final plot settings
    plt.xlabel('kWh')
    plt.ylabel('Total Cost')
    plt.title('$01$: kWh vs. Total Cost')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/01 - kWh vs. Total Cost.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":

    df = prepare_data()
    eda1(df)
