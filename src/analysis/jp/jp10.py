import matplotlib.pyplot as plt
import numpy  as np
import pandas as pd

from analysis.jp.flat        import prepared_data
from analysis.jp.jp04        import remove_anomalies
from analysis.jp.jp06        import lasso
from analysis.jp.jp09        import random_forest
from sklearn.ensemble        import RandomForestRegressor
from sklearn.linear_model    import LinearRegression
from sklearn.model_selection import cross_val_score
from utils.runtime           import find_project_root

def cross_validation(X    : np.ndarray, 
                     y    : pd.Series, 
                     best : RandomForestRegressor):
    '''
    Compares the cross-validation R² scores of the Random Forest and Linear Regression models.

    Methodology:
        1. Perform cross-validation on the Random Forest and Linear Regression models.
        2. Visualize the R² scores for each fold in a bar chart.

    Parameters:
        X    (pd.np.ndarray)         : The transformed training feature set from LASSO feature selection.
        y    (pd.Series)             : The training target variable.
        best (RandomForestRegressor) : The best-fitted Random Forest model from Randomized Search CV.

    Data Science Concepts:
        • Cross-Validation:
            - A resampling procedure used to evaluate machine learning models on a limited data sample.
            - The goal is to estimate the model's performance on an independent dataset and mitigate overfitting.
        • R² Score:
            - A statistical measure representing the proportion of variance for a dependent variable that's explained by 
              an independent variable or variables in a regression model.
            - Indicates the goodness of fit of the model.

    Produces:
        A bar chart saved as a PNG file and displayed on the screen, showing the R² score comparison between the models.
    '''

    # 1: Performing cross-validation
    cv_scores_rf = cross_val_score(best,               X, y, cv = 8, scoring = 'r2', n_jobs = -1)
    cv_scores_lr = cross_val_score(LinearRegression(), X, y, cv = 8, scoring = 'r2', n_jobs = -1)

    # 2: Visualizing the R² scores in a bar chart
    n_folds   = np.arange(1, len(cv_scores_rf) + 1)
    bar_width = 0.35

    bars_rf = plt.bar(n_folds - bar_width/2, cv_scores_rf, bar_width, label = 'Random Forest', color = 'forestgreen')
    bars_lr = plt.bar(n_folds + bar_width/2, cv_scores_lr, bar_width, label = 'Linear Regression')

    # Annotations for each bar
    for bars in [bars_rf, bars_lr]:
        for bar in bars:
            height = bar.get_height()
            plt.text(bar.get_x() + bar.get_width() / 2, 0.02, f'{height:.3f}', 
                     ha = 'center', va = 'bottom',
                     bbox=dict(facecolor = '0', alpha = 0.5, boxstyle = 'round, pad = 0.75'))

    plt.legend(loc = 'upper center', ncol = 2, title_fontproperties = {'weight' : 'bold', 'size' : 10})
    plt.xticks(n_folds)
    plt.xlabel('Fold')
    plt.ylabel('R² Score')
    plt.title('$10$: Cross-Validation R² Scores Comparison')
    plt.tight_layout(pad = 2.0)

    # Saving the plot to a file
    file_path = find_project_root('./fig/analysis/jp/10 - Cross-Validation R2 Scores Comparison.png')
    plt.savefig(file_path)
    plt.show()

if __name__ == "__main__":
    
    df  = prepare_data()
    dfa = remove_anomalies(df)
    X, _, y, _, _ = lasso(dfa)
    best = random_forest(X, y)
    cross_validation(X, y, best)
