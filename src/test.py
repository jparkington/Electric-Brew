import numpy as np
import pandas as pd
from analysis.jp.jp06 import lasso_outputs
from analysis.jp.jp09 import random_forest_outputs
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import cross_val_score

def print_cross_validation_analysis(X: np.ndarray = lasso_outputs['X_train'], 
                                    y: pd.Series = lasso_outputs['y_train'], 
                                    best: RandomForestRegressor = random_forest_outputs['best']):
    """
    Prints the analysis of cross-validation R² scores for Random Forest and Linear Regression models.
    """
    # Performing cross-validation
    cv_scores_rf = cross_val_score(best, X, y, cv=8, scoring='r2', n_jobs=-1)
    cv_scores_lr = cross_val_score(LinearRegression(), X, y, cv=8, scoring='r2', n_jobs=-1)

    # Calculating averages and standard deviations
    avg_score_rf, std_dev_rf = np.mean(cv_scores_rf), np.std(cv_scores_rf)
    avg_score_lr, std_dev_lr = np.mean(cv_scores_lr), np.std(cv_scores_lr)

    # Printing R² scores for each fold
    print("Random Forest Cross-Validation R² Scores:", cv_scores_rf)
    print("Linear Regression Cross-Validation R² Scores:", cv_scores_lr)

    # Printing average R² score and standard deviation
    print("\nRandom Forest - Average R² Score:", avg_score_rf, ", Standard Deviation:", std_dev_rf)
    print("Linear Regression - Average R² Score:", avg_score_lr, ", Standard Deviation:", std_dev_lr)

    # Overall comparison
    print("\nOverall Comparison:")
    print("The Random Forest model shows an average R² score of", avg_score_rf,
          "with a standard deviation of", std_dev_rf,
          ".\nThe Linear Regression model has an average R² score of", avg_score_lr,
          "with a standard deviation of", std_dev_lr,
          ".\nThese results suggest that the Random Forest model is",
          "more consistent and generally more effective in predicting energy costs" if avg_score_rf > avg_score_lr else "less consistent and generally less effective in predicting energy costs",
          "compared to the Linear Regression model.")

if __name__ == "__main__":
    print_cross_validation_analysis()
