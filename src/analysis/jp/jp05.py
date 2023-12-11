import matplotlib.pyplot as plt
import numpy   as np
import pandas  as pd
import seaborn as sns

from analysis.jp.jp04 import without_anomalies
from utils.runtime    import find_project_root

def multicollinearity(df: pd.DataFrame = without_anomalies):
    '''
    Plots a heatmap of the correlation matrix for numeric columns, focusing on high correlations.

    Methodology:
        1. Select numeric columns and filter out 'id' columns and 'total_cost'.
        2. Compute the correlation matrix.
        3. Create a heatmap, masking values below a high correlation threshold.

    Data Science Concepts:
        • Correlation:
            - A statistical measure that expresses the extent to which two variables linearly relate to each other.
            - Values range from -1 (perfect negative correlation) to 1 (perfect positive correlation).
        • Multicollinearity:
            - Occurs when two or more predictors in a model are highly correlated.
            - Hinders tue assessment of the effect of independent variables on dependent variables in regression models.

    Parameters:
        df (pd.DataFrame): The dataframe returned from the `remove_anomalies` function.

    Produces:
        A heatmap saved as a PNG file and displayed on the screen, showing high correlations between numeric columns.
    '''

    # 1: Preparing the DataFrame with just numeric columns
    numeric_df = df.select_dtypes(include = np.number) \
                   .filter(regex = '^(?!.*id).*$') \
                   .drop('total_cost', axis=1)

    # 2: Computing the correlation matrix
    corr_matrix = numeric_df.corr()
    high_corr_threshold = 0.75
    mask = (np.abs(corr_matrix) < high_corr_threshold) | (np.eye(len(corr_matrix)) == 1)

    # 3: Creating the heatmap
    sns.heatmap(corr_matrix, 
                mask  = mask, 
                cmap  = 'coolwarm', 
                annot = True)

    plt.grid(False)
    plt.title('$05$: Multicollinear Facts with High Correlations')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/05 - Multicollinear Facts with High Correlations.png')
    plt.savefig(file_path)
    plt.show()
    

if __name__ == "__main__":
    
    multicollinearity()
