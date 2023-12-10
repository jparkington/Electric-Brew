import matplotlib.pyplot as plt
import pandas  as pd
import seaborn as sns

from analysis.jp.flat import prepared_data
from utils.runtime    import find_project_root

def eda3(df: pd.DataFrame = prepared_data):
    '''
    Plots a scatter plot to visualize the average cost by period over time.

    Methodology:
        1. Group the dataframe by 'period' and 'date', and calculate the mean 'total_cost'.
        2. Use Seaborn's scatterplot to visualize the average cost by period over time.

    Parameters:
        df (pd.DataFrame): The dataframe containing 'period', 'date', and 'total_cost' columns.

    Produces:
        A scatter plot saved as a PNG file and displayed on the screen.
    '''

    # 1: Grouping the data and calculating mean total_cost
    dfg = df.groupby(['period', 'date'])['total_cost'].mean().reset_index()

    # 2: Creating the scatter plot
    p = sns.scatterplot(data = dfg[dfg['total_cost'] > 0], 
                        x    = 'date', 
                        y    = 'total_cost', 
                        hue  = 'period',
                        linewidth = 0)

    # Adjusting the legend and plot settings
    plt.legend(title = 'Period', title_fontproperties = {'weight' : 'bold',  'size' : 10})
    plt.xlabel('Date')
    plt.ylabel('Average Cost')
    plt.title('$03$: Average Cost by Period Over Time')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/03 - Average Cost by Period Over Time.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    eda3()
