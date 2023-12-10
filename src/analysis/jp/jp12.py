import matplotlib.pyplot as plt
import numpy   as np
import pandas  as pd
import seaborn as sns

from analysis.jp.flat import prepared_data
from analysis.jp.jp04 import remove_anomalies
from analysis.jp.jp06 import lasso
from analysis.jp.jp09 import random_forest
from analysis.jp.jp11 import slsqp
from typing           import List
from utils.runtime    import find_project_root

def percent_changes(X    : np.ndarray, 
                    sets : List[np.ndarray], 
                    fts  : List[str]):
    '''
    Visualizes the percentage changes in categorical values after optimization.

    Methodology:
        1. Convert the optimized feature sets into a DataFrame.
        2. Calculate the mean of each feature for both original and optimized sets.
        3. Compute and visualize the percentage changes.

    Parameters:
        X    (np.ndarray)       : The transformed training feature set from LASSO feature selection.
        sets (List[np.ndarray]) : The optimized feature sets that meet the desired cost bounds after optimization.
        fts  (List[str])        : A list of feature names after feature selection and transformation by the LASSO model.

    Produces:
        A bar chart saved as a PNG file and displayed on the screen, showing the percent change in categorical features after optimization.
    '''

    # Convert the list of feature arrays into a DataFrame and calculate the means
    optimized_means = pd.DataFrame(sets,        columns = fts).mean()
    original_means  = pd.DataFrame(X.toarray(), columns = fts).mean()

    # Compute and round the percentage changes
    pct_changes = ((optimized_means - original_means) / original_means).sort_values(ascending = False)
    pct_changes = pct_changes[~pct_changes.index.str.startswith('num_')]

    # Barplot visualization
    sns.barplot(x       = pct_changes, 
                y       = pct_changes.index, 
                hue     = pct_changes, 
                legend  = False,
                palette = 'twilight')

    plt.xlabel('Percentage Change (%)')
    plt.ylabel('Feature')
    plt.title('$12$: Percent Change in Categorical Values After Optimization')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/12 - Percent Change in Categorical Features After Optimization.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    df = prepare_data()
    dfa = remove_anomalies(df)
    X, _, y, _, fts = lasso(dfa)
    best = random_forest(X, y)
    sets = slsqp(X, best)

    percent_changes(X, sets, fts)
