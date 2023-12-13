import numpy as np
import pandas as pd
from analysis.jp.jp06 import lasso_outputs
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, RandomizedSearchCV

def extended_random_forest_analysis(X: np.ndarray = lasso_outputs['X_train'], 
                                    y: pd.Series = lasso_outputs['y_train']):
    """
    Extends the original random_forest function to provide additional data for analysis.
    """
    # Splitting the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=0)

    # Initialize the Random Forest Regressor
    rf = RandomForestRegressor(random_state=0)

    # Hyperparameter Grid
    hyperparameter_grid = {'n_estimators'      : [1, 2, 4, 8, 16],
                           'max_depth'         : [1, 2, 4, 8, 16],
                           'min_samples_split' : [2, 4, 8, 16],
                           'min_samples_leaf'  : [1, 2, 4, 8]}

    # Randomized Search with Cross-Validation
    random_search = RandomizedSearchCV(rf, hyperparameter_grid, n_jobs=-1, random_state=0)
    random_search.fit(X_train, y_train)

    # Predictions using the best model
    best = random_search.best_estimator_
    y_pred = best.predict(X_test)

    # Calculating R² and MSE
    r2 = r2_score(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)

    # Printing R² and MSE
    print(f"R² (Coefficient of Determination): {r2:.3f}")
    print(f"MSE (Mean Squared Error): {mse:.3f}")

    # Printing the best hyperparameters
    print("Best Hyperparameters:", random_search.best_params_)

    # Feature Importance
    print("Feature Importance:", best.feature_importances_)

    # Residual Analysis
    residuals = y_test - y_pred
    print("Residuals Summary:\n", residuals.describe())

    # Distribution of Predictions vs Actual Values
    print("Predictions Summary:\n", pd.Series(y_pred).describe())
    print("Actual Values Summary:\n", y_test.describe())

    return best, y_test, y_pred

best_model, y_test, y_pred = extended_random_forest_analysis()

# Optional: Additional code for visualization or further analysis
# ...
