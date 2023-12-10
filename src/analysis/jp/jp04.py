import matplotlib.pyplot as plt
import pandas as pd

from analysis.jp.flat import prepare_data
from sklearn.ensemble import IsolationForest
from utils.runtime    import find_project_root

def remove_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Applies anomaly detection on the 'total_cost' column using Isolation Forest and visualizes the results.

    Methodology:
        1. Fit an Isolation Forest model on the 'total_cost' column to detect anomalies.
        2. Filter the dataframe to remove detected anomalies.
        3. Visualize the data before and after anomaly detection using scatter plots.

    Data Science Concepts:
        • Isolation Forest:
            - An ensemble algorithm based on decision trees.
            - Designed for anomaly detection, it isolates observations by randomly selecting a feature and 
              then randomly selecting a split value between the maximum and minimum values of the selected feature.
            - The 'contamination' parameter estimates the proportion of outliers in the data set.

    Parameters:
        df (pd.DataFrame): The dataframe containing the 'total_cost' column.

    Returns:
        pd.DataFrame: The dataframe after removing anomalies.

    Produces:
        Two scatter plots saved as a PNG file and displayed on the screen, showing the data before and after 
        anomaly detection.
    '''

    # 1: Fitting the Isolation Forest model
    isolation_forest = IsolationForest(contamination = 0.001, n_jobs = -1, random_state = 0)
    outliers = isolation_forest.fit_predict(df[['total_cost']])

    # 2: Filtering the dataframe to remove detected anomalies
    dfo = df[outliers == 1]

    # Data for plotting
    data_for_plotting = {'Before' : df, 
                         'After'  : dfo}

    # 3: Visualizing the data before and after anomaly detection
    _, axs = plt.subplots(2, 1, figsize = (15, 10), sharex = True, sharey = True)

    # Loop through data and plot
    colormap_range = (df['total_cost'].min(), df['total_cost'].max())

    for i, (title, data) in enumerate(data_for_plotting.items()):
        axs[i].scatter(x = data['total_cost'],
                       y = data['timestamp'],
                       c = data['total_cost'],
                       cmap = 'viridis',
                       vmin = colormap_range[0],
                       vmax = colormap_range[1],
                       edgecolor = None)
        
        axs[i].set_title(f"$04$: Total Cost ${title}$ Anomaly Detection")

        if i == len(data_for_plotting) - 1:
            axs[i].set_xlabel('Total Cost')

    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/04 - Applying Anomaly Detection with Total Cost.png')
    plt.savefig(file_path)
    plt.show()

    return dfo

if __name__ == "__main__":
    
    df = prepare_data()
    remove_anomalies(df)
